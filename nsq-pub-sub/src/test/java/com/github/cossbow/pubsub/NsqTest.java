package com.github.cossbow.pubsub;


import com.github.cossbow.nsq.util.NSQUtil;
import org.junit.Assert;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class NsqTest extends NsqBaseTest {

    private static final String TOPIC_1 = "nsq-test-news-1";
    private static final String TOPIC_2 = "nsq-test-news-2";
    private static final String TOPIC_3 = "nsq-test-news-3";
    private static final String TOPIC_4 = "nsq-test-news-4";
    private static final String TOPIC_5 = "nsq-test-news-5";
    private static final String TOPIC_6 = "nsq-test-news-6";


    private static final String DEFAULT_CHANNEL = "ch";


    NsqPublisher nsqPublisher = nsqPublisher();

    NsqSubscriber nsqSubscriber = nsqSubscriber();

    private long limitAttempts(int attempts, int attemptsLimit) {
        if (attempts < attemptsLimit) {
            return 1_000;
        } else {
            return -1;
        }
    }

    private void pub(int base) {
        for (int i = base + 1; i <= base + 100; i++) {
            var news = create(i);
            nsqPublisher.publish(TOPIC_1, news);
            System.out.println("发：" + news);
        }
    }

    @Test
    public void publish() throws InterruptedException {
        CompletableFuture.runAsync(() -> {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            nsqSubscriber.subscribe(TOPIC_1, DEFAULT_CHANNEL, News.class, news -> {
                System.out.println("获取到（一）: " + news.getId() + "; at thread-" + Thread.currentThread().getId());
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

            }, 3);
        });

        Executor executor = Executors.newCachedThreadPool();
        executor.execute(() -> {
            pub(120);
        });


        System.out.println("发——完啦！");
        Thread.sleep(10_000);
    }


    @Test
    public void attemptLimit() throws InterruptedException {
        nsqPublisher.publish(TOPIC_2, "消息通知");

        int attemptsLimit = 4;
        var a = new AtomicInteger(0);
        nsqSubscriber.subscribe(TOPIC_2, DEFAULT_CHANNEL, String.class, news -> {
            System.out.println("获取到（一）: " + news + "; times=" + a.incrementAndGet());
            if (a.get() >= attemptsLimit) {
                synchronized (a) {
                    a.notifyAll();
                }
            }
            throw new NullPointerException();
        }, 1, NsqUtil.attemptLimit(100, attemptsLimit));

        synchronized (a) {
            a.wait();
        }

        Assert.assertEquals(attemptsLimit, a.get());
        System.out.println(MessageFormat.format("limit={0}, value={1}", attemptsLimit, a.get()));
    }

    @Test
    public void attemptCall() throws InterruptedException {
        nsqPublisher.publish(TOPIC_2, "消息通知");

        var lock = new Object();

        int attemptsLimit = 4;
        var a = new AtomicInteger(0);
        nsqSubscriber.subscribe(TOPIC_2, DEFAULT_CHANNEL, String.class, news -> {
            var c = a.incrementAndGet();
            System.out.println("获取到（一）: " + news + "; times=" + c);
            synchronized (lock) {
                if (c == attemptsLimit) {
                    lock.notifyAll();
                }
            }
            throw new RetryDeferEx();
        }, 1, attempts -> limitAttempts(attempts, attemptsLimit));

        synchronized (lock) {
            lock.wait();
        }

        Assert.assertEquals("尝试次数不正确", attemptsLimit, a.get());
        System.out.println(MessageFormat.format("limit={0}, value={1}", attemptsLimit, a.get()));
    }


    @Test
    public void unsubscribe() throws InterruptedException {
        final var a = new AtomicInteger(0);
        final String ch = "ch-1";
        nsqSubscriber.subscribe(TOPIC_3, ch, String.class, s -> {
            System.out.printf("counter=%d, message=%s\n", a.incrementAndGet(), s);
        });
        Thread.sleep(100);
        nsqSubscriber.unsubscribe(TOPIC_3, ch);
        Thread.sleep(100);

        nsqPublisher.publish(TOPIC_3, "message---100010");

        Thread.sleep(1000);
        Assert.assertEquals("not zero", 0, a.get());
    }

    @Test
    public void subscribeAsync() throws InterruptedException {

        final var a = new AtomicInteger(0);
        final String ch = "ch-1";


        int attemptsLimit = 4;

        nsqPublisher.publish(TOPIC_4, "https://www.qq.com/");
        Thread.sleep(100);

        nsqSubscriber.subscribeAsync(TOPIC_4, ch, String.class, s -> {
            System.out.println("GET " + s);
            var f = get(s);

            return f.thenApply(r -> {
                var c = a.incrementAndGet();
                System.out.println("try " + c + " times, re=" + r.length());
                if (c >= attemptsLimit) {
                    synchronized (ch) {
                        ch.notifyAll();
                    }
                    return null;
                } else {
                    System.out.println("retry..." + c);
                    throw new RetryDeferEx();
                }
            });
        }, 1, attempts -> limitAttempts(attempts, attemptsLimit));


        synchronized (ch) {
            ch.wait();
        }
        Thread.sleep(1000);

        Assert.assertEquals("wrong times", attemptsLimit, a.get());

    }

    @Test
    public void multiConsumer() throws InterruptedException {
        String ch = "ch-1";

        var queue = new LinkedBlockingQueue<>();
        nsqSubscriber.subscribe(TOPIC_5, ch, String.class, s -> {
            System.out.println("recv1: " + s);
            try {
                queue.put(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 2);
        nsqSubscriber.subscribe(TOPIC_5, ch, String.class, s -> {
            System.out.println("recv2: " + s);
            try {
                queue.put(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 3);

        var total = 100;
        for (int i = 0; i < total; i++) {
            nsqPublisher.publish(TOPIC_5, "message-" + i);
        }

        int j = 0;
        while (true) {
            var v = queue.poll(1000, TimeUnit.MILLISECONDS);
            if (null == v) {
                break;
            }
            j++;
        }
        Assert.assertEquals(total, j);
    }


    static class Book {
        private int id;
        private String name;

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    @Test
    public void serializeJSON() throws InterruptedException {
        final AtomicInteger counter = new AtomicInteger(0);

        nsqSubscriber.subscribe(TOPIC_6, "c", Book.class, NSQUtil::fromJson, b -> {
            try {
                System.out.println(b);
            } finally {
                counter.incrementAndGet();
            }
            synchronized (TOPIC_6) {
                TOPIC_6.notify();
            }
        }, 1);

        var b = new Book();
        b.id = ThreadLocalRandom.current().nextInt(1000, 99999);
        b.name = "C++ Primary";
        var publisher = EncoderPublisher.wrapEncoder(nsqPublisher, NSQUtil::writeJson);
        publisher.publish(TOPIC_6, b);

        synchronized (TOPIC_6) {
            TOPIC_6.wait();
        }

        Assert.assertEquals(1, counter.get());
    }

}
