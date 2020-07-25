package com.github.cossbow.nsq;

import com.github.cossbow.nsq.exceptions.NSQException;
import com.github.cossbow.nsq.exceptions.NoConnectionsException;
import com.github.cossbow.nsq.lookup.DefaultNSQLookup;
import com.github.cossbow.nsq.lookup.NSQLookup;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class NSQConsumerTest extends Nsq {
    private final static Logger log = LoggerFactory.getLogger(NSQConsumerTest.class);

    //duration to wait before auto-requeing a message setting in nsqd, defined with -msg-timeout:
    //Set your timeout -msg-timeout="5s" command line when starting nsqd or changed this constant
    //to the default of 60000.
    private static final long NSQ_MSG_TIMEOUT = 5000;

    @Test
    public void testDecodeString() throws IOException {
        var in = new ByteArrayInputStream("Hello".getBytes());
        var s = STRING_DECODER.apply(in);
        Assert.assertEquals(s, "Hello");
    }

    @Test
    public void testLongRunningConsumer() throws NSQException, TimeoutException, InterruptedException {
        final var counter = new AtomicInteger(0);
        final var topic = "nsq-client-test-1";
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        var consumer = new NSQConsumer<>(lookup, topic, "testconsumer", 1, (message) -> {
            var c = counter.incrementAndGet();
            log.info("Processing message: {}, counter={}", new String(message.getMessage()), c);

            synchronized (counter) {
                if (c >= 2) {
                    counter.notifyAll();
                    log.info("完啦！");
                } else {
                    message.requeue(1_000);
                    log.info("还没完");
                }
            }
        }, CONFIG, STRING_DECODER);
        consumer.start();

        var producer = new NSQProducer();
        producer.addAddress(Nsq.getNsqdHost(), 4150);
        producer.start();
        String msg = "test-one-message";
        producer.produce(topic, msg.getBytes());
        producer.shutdown();

        synchronized (counter) {
            counter.wait();
        }

        assertTrue("数量不对", counter.get() == 2);
        consumer.shutdown();
    }


    @Test
    public void testConcurrencyConsumer() throws InterruptedException {
        final var counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        final var topic = "nsq-client-test-0";
        final int messageTotal = 41;
        final Object notifyFinish = new Object();

        CompletableFuture.runAsync(() -> {
            lookup.lookupNodeAsync().thenAccept(addresses -> {
                try {
                    var producer = new NSQProducer();
                    producer.addAddresses(addresses);
                    producer.start();

                    for (int i = 0; i < messageTotal; i++) {
                        String msg = "message-" + i;
                        System.out.println("send: " + msg);
                        producer.produceAsync(topic, msg.getBytes());
                    }
                } catch (NoConnectionsException e) {
                    throw new IllegalStateException(e);
                }
            });
        });
        Thread.sleep(1000);


        {
            var consumer = new NSQConsumer<>(lookup, topic, "test-consumer", 2, message -> {
                String m = new String(message.getMessage());
                printWithOrder("get: " + m);
                sleep(1000);
                message.finished();
            }, CONFIG, STRING_DECODER, Throwable::printStackTrace);
            consumer.start();
        }
        {
            var consumer = new NSQConsumer<>(lookup, topic, "test-consumer", 4, message -> {
                String m = new String(message.getMessage());
                printWithOrder("get: " + m);
                sleep(2000);
                message.finished();

            }, CONFIG, STRING_DECODER, Throwable::printStackTrace);
            consumer.start();
        }

        executor.schedule(() -> {
            synchronized (notifyFinish) {
                notifyFinish.notifyAll();
            }
        }, 10, TimeUnit.MINUTES);

        synchronized (notifyFinish) {
            notifyFinish.wait();
        }
        assertEquals(counter.get(), messageTotal);
    }


    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);

    private void printWithOrder(Object v) {
        executor.execute(() -> System.out.println(v));
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
