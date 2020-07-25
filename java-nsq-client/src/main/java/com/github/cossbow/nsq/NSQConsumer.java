package com.github.cossbow.nsq;


import com.github.cossbow.nsq.exceptions.NSQException;
import com.github.cossbow.nsq.exceptions.NoConnectionsException;
import com.github.cossbow.nsq.lookup.NSQLookup;
import com.github.cossbow.nsq.util.NSQUtil;
import com.github.cossbow.nsq.util.ThrowoutFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;


public class NSQConsumer<T> implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(NSQConsumer.class);


    private final NSQLookup lookup;
    private final String topic;
    private final String channel;
    private final ThrowoutFunction<InputStream, T, IOException> decoder;
    private final Consumer<NSQMessage<T>> callback;
    private final Consumer<NSQException> errorCallback;
    private final NSQConfig config;
    private volatile long nextTimeout = 0;
    private final Map<ServerAddress, Connection> connections = new ConcurrentHashMap<>();
    private final AtomicLong totalMessages = new AtomicLong(0L);

    private volatile boolean started = false;
    private int messagesPerBatch;
    private long lookupPeriod = 60 * 1000; // how often to recheck for new nodes (and clean up non responsive nodes)
    private ExecutorService executor = NSQUtil.DEFAULT_EXECUTOR;
    private volatile ScheduledFuture<?> timeoutFuture = null;


    public NSQConsumer(final NSQLookup lookup, final String topic, final String channel, final int concurrency, final Consumer<NSQMessage<T>> callback,
                       final NSQConfig config, final ThrowoutFunction<InputStream, T, IOException> decoder) {
        this(lookup, topic, channel, concurrency, callback, config, decoder, null);
    }


    public NSQConsumer(final NSQLookup lookup, final String topic, final String channel, final int concurrency, final Consumer<NSQMessage<T>> callback,
                       final NSQConfig config, final ThrowoutFunction<InputStream, T, IOException> decoder, final Consumer<NSQException> errCallback) {
        this.lookup = lookup;
        this.topic = topic;
        this.channel = channel;
        this.config = config.clone();
        this.callback = callback;
        this.errorCallback = errCallback;
        this.decoder = Objects.requireNonNull(decoder);
        this.messagesPerBatch = config.getMaxInFlight().orElse(concurrency);
    }

    public NSQConsumer<T> start() {
        if (!started) {
            started = true;
            //connect once otherwise we might have to wait one lookupPeriod
            connect();
            NSQUtil.SCHEDULER.scheduleAtFixedRate(this::connect, lookupPeriod, lookupPeriod, TimeUnit.MILLISECONDS);
        }
        return this;
    }

    private Connection createConnection(final ServerAddress serverAddress) {
        try {
            final Connection connection = new Connection(serverAddress, config);

            connection.setConsumer(this);
            connection.setErrorCallback(errorCallback);
            connection.command(NSQCommand.subscribe(topic, channel));
            connection.command(NSQCommand.ready(messagesPerBatch));

            return connection;
        } catch (final NoConnectionsException e) {
            log.warn("Could not create connection to server {}", serverAddress.toString(), e);
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.info("thread interrupted, return null");
            return null;
        }
    }

    protected void processMessage(final NSQMessage<T> message) {
        if (callback == null) {
            message.release();
            log.warn("NO Callback, dropping message: " + message);
        } else {
            try (var in = message.newMessageStream()) {
                var t = decoder.apply(in);
                message.setObj(t);
            } catch (Throwable e) {
                log.error("decode message body", e);
                message.finished();
            } finally {
                message.release();
            }
            try {
                executor.execute(() -> callback.accept(message));
                if (nextTimeout > 0) {
                    updateTimeout(message, -500);
                }
            } catch (RejectedExecutionException re) {
                log.trace("Backing off");
                message.requeue();
                updateTimeout(message, 500);
            }
        }

        final long tot = totalMessages.incrementAndGet();
        if (tot % messagesPerBatch > (messagesPerBatch / 2)) {
            //request some more!
            rdy(message, messagesPerBatch);
        }
    }

    private void updateTimeout(final NSQMessage<T> message, long change) {
        rdy(message, 0);
        log.trace("RDY 0! Halt Flow.");
        if (null != timeoutFuture) {
            final var future = timeoutFuture;
            future.cancel(false);
            timeoutFuture = null;
        }
        var newTimeout = calculateTimeoutDate(change);
        if (newTimeout) {
            timeoutFuture = NSQUtil.SCHEDULER.schedule(() -> {
                rdy(message, 1); // test the waters
            }, 0, TimeUnit.MILLISECONDS);
        }
    }

    private void rdy(final NSQMessage<T> message, int size) {
        message.getConnection().command(NSQCommand.ready(size));
    }

    private boolean calculateTimeoutDate(final long i) {
        if (System.currentTimeMillis() - nextTimeout + i > 50) {
            nextTimeout += i;
            return true;
        } else {
            nextTimeout = 0;
            return false;
        }
    }

    public void shutdown() {
        cleanClose();
    }

    private void cleanClose() {
        final NSQCommand command = NSQCommand.startClose();
        connections.forEach((address, connection) -> {
            var future = connection.command(command);
            future.addListener(f -> {
                if (f.isSuccess()) {
                    log.info("CLOSE {} finish", address);
                    connection.close();
                } else {
                    log.error("CLOSE " + address + " error", f.cause());
                }
            });
        });
    }

    public NSQConsumer<T> setMessagesPerBatch(final int messagesPerBatch) {
        if (!started) {
            this.messagesPerBatch = messagesPerBatch;
        }
        return this;
    }

    public synchronized NSQConsumer<T> setLookupPeriod(final long periodMillis) {
        if (!started) {
            this.lookupPeriod = periodMillis;
        }
        return this;
    }


    private void connect() {
        lookup.lookupAsync(topic).thenAcceptAsync(newAddresses -> {
            for (final var it = connections.entrySet().iterator(); it.hasNext(); ) {
                Connection cnn = it.next().getValue();
                if (!cnn.isConnected() || !cnn.isHeartbeatStatusOK()) {
                    //force close
                    cnn.close();
                    it.remove();
                }
            }

            final var oldAddresses = connections.keySet();

            log.debug("Addresses NSQ connected to: " + newAddresses);
            if (newAddresses.isEmpty()) {
                // in case the lookup server is not reachable for a short time we don't we dont want to
                // force close connection
                // just log a message and keep moving
                log.debug("No NSQLookup server connections or topic does not exist. Try latter...");
            } else {
                Set<ServerAddress> diff = new HashSet<>(oldAddresses);
                diff.removeAll(newAddresses);
                for (final ServerAddress server : diff) {
                    log.info("Remove connection " + server.toString());
                    Optional.of(connections.remove(server)).ifPresent(Connection::close);
                }

                diff.clear();
                diff.addAll(newAddresses);
                diff.removeAll(oldAddresses);
                diff.forEach(server -> {
                    final Connection connection = createConnection(server);
                    if (connection != null) {
                        connections.put(server, connection);
                    }
                });

                diff.clear();
            }
        }).exceptionally(e -> {
            if (e instanceof CompletionException) e = e.getCause();
            log.error("lookup error", e);
            return null;
        });
    }

    public long getTotalMessages() {
        return totalMessages.get();
    }

    /**
     * This is the executor where the callbacks happen.
     * The executer can only changed before the client is started.
     * Default is a cached threadpool.
     */
    public synchronized NSQConsumer<T> setExecutor(final ExecutorService executor) {
        if (!started) {
            this.executor = executor;
        }
        return this;
    }

    public synchronized Executor getExecutor() {
        return Objects.requireNonNullElse(executor, NSQUtil.DEFAULT_EXECUTOR);
    }

    private Set<ServerAddress> lookupAddresses() {
        return lookup.lookup(topic);
    }

    @Override
    public void close() throws IOException {
        shutdown();
    }
}
