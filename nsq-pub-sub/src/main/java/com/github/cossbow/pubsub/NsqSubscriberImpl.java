package com.github.cossbow.pubsub;

import com.github.cossbow.nsq.NSQConfig;
import com.github.cossbow.nsq.NSQConsumer;
import com.github.cossbow.nsq.NSQMessage;
import com.github.cossbow.nsq.exceptions.NSQException;
import com.github.cossbow.nsq.lookup.NSQLookup;
import com.github.cossbow.nsq.util.ThrowoutFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntToLongFunction;
import java.util.function.Supplier;


public
class NsqSubscriberImpl implements NsqSubscriber {
    private static final Logger log = LoggerFactory.getLogger(NsqSubscriberImpl.class);

    private static final Consumer<NSQException> EXCEPTION_HANDLER = ex -> log.warn("NSQ Error", ex);

    private final NSQLookup nsqLookup;

    private long lookupPeriodMillis;
    private int defaultAttemptLimit;
    private int defaultAttemptDelay;

    private String userAgent;

    private final Map<String, Map<String, NSQConsumer<?>>> consumers = new ConcurrentHashMap<>();

    public NsqSubscriberImpl(
            NSQLookup nsqLookup, long lookupPeriodMillis, int defaultAttemptLimit,
            int defaultAttemptDelay, int schedulerPoolSize,
            String userAgent) {
        if (lookupPeriodMillis <= 0) {
            throw new IllegalArgumentException("'lookupPeriodMillis' must be positive");
        }
        if (defaultAttemptLimit < 0) {
            throw new IllegalArgumentException("'defaultAttemptLimit' can not be negative");
        }
        if (defaultAttemptDelay < 0) {
            throw new IllegalArgumentException("'defaultAttemptDelay' can not be negative");
        }
        if (schedulerPoolSize <= 0) {
            throw new IllegalArgumentException("'schedulerPoolSize' must be positive");
        }


        this.nsqLookup = nsqLookup;

        this.lookupPeriodMillis = lookupPeriodMillis;
        this.defaultAttemptLimit = defaultAttemptLimit;
        this.defaultAttemptDelay = defaultAttemptDelay;
        this.userAgent = userAgent;

    }

    private NSQConfig newConfig() {
        var config = new NSQConfig();
        config.setUserAgent(userAgent);
        return config;
    }


    private void addConsumer(String topic, String channel, Supplier<NSQConsumer<?>> supplier) {
        synchronized (consumers) {
            final var m = consumers.computeIfAbsent(topic, t -> new ConcurrentHashMap<>());
            if (m.containsKey(channel)) {
                throw new IllegalStateException(topic + "[" + channel + "] has been subscribed");
            }
            m.put(channel, supplier.get());
        }
    }

    private long attemptsDelay(int attempts) {
        return 1_000 * (4 << attempts);
    }

    private IntToLongFunction getAttemptsDelay(int attemptsLimit) {
        if (attemptsLimit < 0) {
            // not attempt
            return NsqUtil.attemptNot();
        } else if (attemptsLimit == 0) {
            // attempts always, try once per default
            return NsqUtil.attemptAlways(defaultAttemptDelay);
        } else {
            // attempts with limit, by delay
            return NsqUtil.attemptLimit(this::attemptsDelay, attemptsLimit);
        }
    }

    @Override
    public <T> ThrowoutFunction<InputStream, T, IOException> newDecoder(Decoder decoder, Class<T> type) {
        return is -> decoder.decode(is, type);
    }

    @Override
    public <T> void subscribe(String topic, String channel, ThrowoutFunction<InputStream, T, IOException> decoder, Consumer<T> consumer, int concurrency) {
        subscribe(topic, channel, decoder, consumer, concurrency, getAttemptsDelay(defaultAttemptLimit));
    }

    public <T> void subscribe(String topic, String channel, Class<T> type, Decoder decoder, Consumer<T> consumer, int concurrency, int attemptsLimit) {
        subscribe(topic, channel, type, decoder, consumer, concurrency, getAttemptsDelay(attemptsLimit));
    }

    @Override
    public <T> void subscribe(String topic, String channel, ThrowoutFunction<InputStream, T, IOException> decoder, Consumer<T> consumer, int concurrency, IntToLongFunction attemptDelay) {
        if (concurrency <= 0) {
            throw new IllegalArgumentException("threads must not negative");
        }

        addConsumer(topic, channel, () -> {
            final Consumer<NSQMessage<T>> callback = message -> {
                try {
                    var t = message.getObj();
                    log.trace("consume({}) message: {}", topic, t);
                    consumer.accept(t);
                    message.finished();
                } catch (Throwable e) {
                    dealErrorOrAttempt(message, attemptDelay, topic, e);
                }
            };
            var config = newConfig();
            var c = new NSQConsumer<>(nsqLookup, topic, channel, concurrency, callback, config, decoder, EXCEPTION_HANDLER);
            c.setLookupPeriod(lookupPeriodMillis);
            c.start();
            return c;
        });

    }


    //
    //
    //


    //
    //
    //

    @Override
    public <T> void subscribeAsync(String topic, String channel, Class<T> type, Function<T, CompletableFuture<?>> process, int concurrency) {
        subscribeAsync(topic, channel, type, process, concurrency, defaultAttemptLimit);
    }

    @Override
    public <T> void subscribeAsync(String topic, String channel, Class<T> type, Function<T, CompletableFuture<?>> process, int concurrency, int attemptsLimit) {
        subscribeAsync(topic, channel, type, process, concurrency, getAttemptsDelay(attemptsLimit));
    }


    @Override
    public <T> void subscribeAsync(String topic, String channel, ThrowoutFunction<InputStream, T, IOException> decoder, Function<T, CompletableFuture<?>> process,
                                   int concurrency, IntToLongFunction attemptDelay) {
        if (concurrency <= 0) {
            throw new IllegalArgumentException("threads must not negative");
        }

        addConsumer(topic, channel, () -> {
            final Consumer<NSQMessage<T>> callback = message -> {
                try {
                    var t = message.getObj();
                    log.trace("consume({}) message: {}", topic, t);
                    var future = process.apply(t);
                    if (null == future) {
                        message.finished();
                        return;
                    }
                    future.whenComplete((v, ex) -> {
                        if (null == ex) {
                            // success
                            message.finished();
                        } else {
                            if (ex instanceof CompletionException) ex = ex.getCause();
                            dealErrorOrAttempt(message, attemptDelay, topic, ex);
                        }
                    });

                } catch (Throwable e) {
                    dealErrorOrAttempt(message, attemptDelay, topic, e);
                }
            };
            var config = newConfig();
            var c = new NSQConsumer<>(nsqLookup, topic, channel, concurrency, callback, config, decoder, EXCEPTION_HANDLER);
            c.setLookupPeriod(lookupPeriodMillis);
            c.start();
            return c;
        });
    }

    private void dealErrorOrAttempt(NSQMessage<?> message, IntToLongFunction attemptDelay, String topic, Throwable ex) {
        long delay = attemptDelay.applyAsLong(message.getAttempts());
        boolean onlyRetry = ex instanceof RetryDeferEx;
        if (onlyRetry) {
            delay = ((RetryDeferEx) ex).getDefer(attemptDelay.applyAsLong(message.getAttempts()));
        } else if (!(ex instanceof RuntimeException)) {
            message.finished();
            log.error("consume(" + topic + ") message error, no attempt", ex);
            return;
        }
        // exception occur, retry

        if (delay > 0) {
            message.requeue((int) delay);
            if (onlyRetry) {
                log.debug("consume({}) message need retry: attempt {} times, try {}ms latter", topic, message.getAttempts(), delay);
            } else {
                log.warn("consume({}) message error: attempt {} times, try {}ms latter", topic, message.getAttempts(), delay);
            }
        } else if (delay == 0) {
            message.requeue();
            if (onlyRetry) {
                log.debug("consume({}) message need retry: attempt {} times, try immediately", topic, message.getAttempts());
            } else {
                log.warn("consume({}) message error: attempt {} times, try immediately", topic, message.getAttempts());
            }
        } else {
            message.finished();
            if (onlyRetry) {
                log.info("consume({}) message need retry: attempt out times", topic);
            } else {
                log.error("consume({}) message error: attempt out times, {}", topic, ex.getMessage());
            }
        }
    }


    //
    //
    //

    @Override
    public void unsubscribe(String topic, String channel) {
        synchronized (consumers) {
            var m = consumers.get(topic);
            if (null == m) {
                return;
            }
            var c = m.remove(channel);
            if (null == c) {
                return;
            }
            try {
                c.close();
            } catch (IOException e) {
                log.error("close", e);
            }
        }
    }


}
