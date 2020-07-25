package com.github.cossbow.nsq;


import com.github.cossbow.nsq.exceptions.NSQException;
import com.github.cossbow.nsq.exceptions.NoConnectionsException;
import com.github.cossbow.nsq.lookup.DefaultNSQLookup;
import com.github.cossbow.nsq.lookup.NSQLookup;
import com.github.cossbow.nsq.util.ThrowoutConsumer;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class NSQProducerTest extends Nsq {
    private final static Logger log = LoggerFactory.getLogger(NSQProducerTest.class);

    private static int concurrency = 1;

    private NSQConfig getSnappyConfig() {
        final NSQConfig config = new NSQConfig();
        config.setCompression(NSQConfig.Compression.SNAPPY);
        return config;
    }

    private NSQConfig getDeflateConfig() {
        final NSQConfig config = new NSQConfig();
        config.setCompression(NSQConfig.Compression.DEFLATE);
        config.setDeflateLevel(4);
        return config;
    }

    private NSQConfig getSslConfig() throws SSLException {
        final NSQConfig config = new NSQConfig();
        File serverKeyFile = new File(getClass().getResource("/server.pem").getFile());
        File clientKeyFile = new File(getClass().getResource("/client.key").getFile());
        File clientCertFile = new File(getClass().getResource("/client.pem").getFile());
        SslContext ctx = SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL).trustManager(serverKeyFile)
                .keyManager(clientCertFile, clientKeyFile).build();
        config.setSslContext(ctx);
        return config;
    }

    private NSQConfig getSslAndSnappyConfig() throws SSLException {
        final NSQConfig config = getSslConfig();
        config.setCompression(NSQConfig.Compression.SNAPPY);
        return config;
    }

    private NSQConfig getSslAndDeflateConfig() throws SSLException {
        final NSQConfig config = getSslConfig();
        config.setCompression(NSQConfig.Compression.DEFLATE);
        config.setDeflateLevel(4);
        return config;
    }

    @Test
    public void testProduceOneMsgSnappy() throws NSQException, TimeoutException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        NSQProducer producer = new NSQProducer();
        producer.setConfig(getSnappyConfig());
        producer.addAddress(Nsq.getNsqdHost(), 4150);
        producer.start();
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.shutdown();

        var consumer = new NSQConsumer<>(lookup, "test3", "testconsumer", concurrency, (message) -> {
            log.info("Processing message: " + (message.getObj()));
            counter.incrementAndGet();
            message.finished();
        }, getSnappyConfig(), STRING_DECODER);
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.shutdown();
    }

    @Test
    public void testProduceOneMsgDeflate() throws NSQException, TimeoutException, InterruptedException {
        System.setProperty("io.netty.noJdkZlibDecoder", "false");
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        NSQProducer producer = new NSQProducer();
        producer.setConfig(getDeflateConfig());
        producer.addAddress(Nsq.getNsqdHost(), 4150);
        producer.start();
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.shutdown();

        var consumer = new NSQConsumer<>(lookup, "test3", "testconsumer", concurrency, (message) -> {
            log.info("Processing message: " + (message.getObj()));
            counter.incrementAndGet();
            message.finished();
        }, getDeflateConfig(), STRING_DECODER);
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.shutdown();
    }

    @Test
    public void testProduceOneMsgSsl() throws InterruptedException, NSQException, TimeoutException, SSLException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        NSQProducer producer = new NSQProducer();
        producer.setConfig(getSslConfig());
        producer.addAddress(Nsq.getNsqdHost(), 4150);
        producer.start();
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.shutdown();

        var consumer = new NSQConsumer<>(lookup, "test3", "testconsumer", concurrency, (message) -> {
            log.info("Processing message: " + message.getObj());
            counter.incrementAndGet();
            message.finished();
        }, getSslConfig(), STRING_DECODER);
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.shutdown();
    }

    @Test
    public void testProduceOneMsgSslAndSnappy() throws InterruptedException, NSQException, TimeoutException, SSLException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        NSQProducer producer = new NSQProducer();
        producer.setConfig(getSslAndSnappyConfig());
        producer.addAddress(Nsq.getNsqdHost(), 4150);
        producer.start();
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.shutdown();

        var consumer = new NSQConsumer<>(lookup, "test3", "testconsumer", concurrency, (message) -> {
            log.info("Processing message: " + message.getObj());
            counter.incrementAndGet();
            message.finished();
        }, getSslAndSnappyConfig(), STRING_DECODER);
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.shutdown();
    }

    @Test
    public void testProduceOneMsgSslAndDeflat() throws InterruptedException, NSQException, TimeoutException, SSLException {
        System.setProperty("io.netty.noJdkZlibDecoder", "false");
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        NSQProducer producer = new NSQProducer();
        producer.setConfig(getSslAndDeflateConfig());
        producer.addAddress(Nsq.getNsqdHost(), 4150);
        producer.start();
        String msg = randomString();
        producer.produce("test3", msg.getBytes());
        producer.shutdown();

        var consumer = new NSQConsumer<>(lookup, "test3", "testconsumer", concurrency, (message) -> {
            log.info("Processing message: " + message.getObj());
            counter.incrementAndGet();
            message.finished();
        }, getSslAndDeflateConfig(), STRING_DECODER);
        consumer.start();
        while (counter.get() == 0) {
            Thread.sleep(500);
        }
        assertEquals(1, counter.get());
        consumer.shutdown();
    }


    @Test
    public void testProduceMoreMsg() throws NSQException, TimeoutException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        var consumer = new NSQConsumer<>(lookup, "test3", "testconsumer", concurrency, (message) -> {
            log.info("Processing message: " + message.getObj());
            counter.incrementAndGet();
            message.finished();
        }, CONFIG, STRING_DECODER);
        consumer.start();

        NSQProducer producer = new NSQProducer();
        producer.addAddress(Nsq.getNsqdHost(), 4150);
        producer.start();
        for (int i = 0; i < 1000; i++) {
            String msg = randomString();
            producer.produce("test3", msg.getBytes());
        }
        producer.shutdown();

        while (counter.get() < 1000) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 1000);
        consumer.shutdown();
    }

    @Test
    public void testParallelProducer() throws NSQException, TimeoutException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        var consumer = new NSQConsumer<>(lookup, "test3", "testconsumer", concurrency, (message) -> {
            log.info("Processing message: " + message.getObj());
            counter.incrementAndGet();
            message.finished();
        }, CONFIG, STRING_DECODER);
        consumer.start();

        NSQProducer producer = new NSQProducer();
        producer.addAddress(Nsq.getNsqdHost(), 4150);
        producer.start();
        for (int n = 0; n < 5; n++) {
            new Thread(() -> {
                for (int i = 0; i < 1000; i++) {
                    String msg = randomString();
                    try {
                        producer.produce("test3", msg.getBytes());
                    } catch (NSQException | TimeoutException e) {
                        log.error("produce", e);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }).start();
        }
        while (counter.get() < 5000) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 5000);
        producer.shutdown();
        consumer.shutdown();
    }

    @Test
    public void testMultiMessage() throws NSQException, TimeoutException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);
        final var topic = "test3";

        var consumer = new NSQConsumer<>(lookup, topic, "testconsumer", concurrency, (message) -> {
            log.info("Processing message: " + message.getObj());
            counter.incrementAndGet();
            message.finished();
        }, CONFIG, STRING_DECODER);
        consumer.start();

        NSQProducer producer = new NSQProducer();
        producer.addAddress(Nsq.getNsqdHost(), 4150);
        producer.start();
        List<byte[]> messages = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            messages.add((randomString() + "-" + i).getBytes());
        }
        producer.produceMulti(topic, messages);
        producer.shutdown();

        while (counter.get() < 50) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 50);
        consumer.shutdown();
    }

    @Test
    public void testMultiMessageStream() throws NSQException, TimeoutException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);
        final var topic = "test3-s";
        final var testLen = 10;

        var queue=new LinkedBlockingQueue<String>();
        var consumer = new NSQConsumer<>(lookup, topic, "testconsumer", concurrency, (message) -> {
            log.info("Processing message: " + message.getObj());
            var c = counter.incrementAndGet();
            message.finished();
            if (c >= testLen) {
                synchronized (counter) {
                    counter.notify();
                }
            }
        }, CONFIG, STRING_DECODER);
        consumer.start();

        NSQProducer producer = new NSQProducer();
        producer.addAddress(Nsq.getNsqdHost(), 4150);
        producer.start();
        var sb = Stream.<ThrowoutConsumer<OutputStream, IOException>>builder();
        for (int i = 0; i < testLen; i++) {
            var m = randomString() + "-" + i;
            sb.accept(os -> {
                os.write(m.getBytes());
                os.close();
            });
        }

        producer.produceMulti(topic, sb.build());
        producer.shutdown();


        assertEquals(counter.get(), testLen);
        consumer.shutdown();
    }

    @Test
    public void testBackoff() throws InterruptedException, NSQException, TimeoutException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        var consumer = new NSQConsumer<>(lookup, "test3", "testconsumer", concurrency, (message) -> {
            log.info("Processing message: " + (message.getObj()));
            counter.incrementAndGet();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            message.finished();
        }, CONFIG, STRING_DECODER);
//        consumer.setExecutor(newBackoffThreadExecutor());
        consumer.start();

        NSQProducer producer = new NSQProducer();
        producer.addAddress(Nsq.getNsqdHost(), 4150);
        producer.start();
        for (int i = 0; i < 20; i++) {
            String msg = randomString();
            producer.produce("test3", msg.getBytes());
        }
        producer.shutdown();

        while (counter.get() < 20) {
            Thread.sleep(500);
        }
        assertTrue(counter.get() >= 20);
        consumer.shutdown();
    }

    @Test
    public void testScheduledCallback() throws NSQException, TimeoutException, InterruptedException {
        AtomicInteger counter = new AtomicInteger(0);
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        var consumer = new NSQConsumer<>(lookup, "test3", "testconsumer", concurrency, (message) -> {
        }, CONFIG, STRING_DECODER);
        consumer.start();

        Thread.sleep(1000);
        assertTrue(counter.get() == 1);
        consumer.shutdown();
    }

    @Test
    public void testEphemeralTopic() throws InterruptedException, NSQException, TimeoutException {
        NSQLookup lookup = new DefaultNSQLookup();
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);

        NSQProducer producer = new NSQProducer();
        producer.setConfig(getDeflateConfig());
        producer.addAddress(Nsq.getNsqdHost(), 4150);
        producer.start();
        String msg = randomString();
        producer.produce("testephem#ephemeral", msg.getBytes());
        producer.shutdown();

        Set<ServerAddress> servers = lookup.lookupAsync("testephem#ephemeral").join();
        assertEquals("Could not find servers for ephemeral topic", 1, servers.size());
    }

    @Test
    public void testDeferredMessage() throws NoConnectionsException, InterruptedException {
        final var topic = "message-deferred-1";
        final int defer = 3;

        var cf = new CompletableFuture<Void>();
        var consumer = new NSQConsumer<>(lookup, topic, "default", 1, msg -> {
            try {
                var now = LocalTime.now();
                var data = msg.getObj();
                var st = LocalTime.parse(data);
                var dt = now.getSecond() - st.getSecond();

                System.out.println(now + " - " + st + " = " + dt);

                if (defer != dt) {
                    cf.completeExceptionally(new IllegalStateException("expect " + defer + "ms, but actual " + dt + "ms"));
                }

                msg.finished();
            } catch (Exception e) {
                msg.requeue();
            } finally {
                if (!cf.isDone()) {
                    cf.complete(null);
                }
            }
        }, CONFIG, STRING_DECODER);
        consumer.start();

        var producer = newProducer();

        producer.produceAsync(topic, defer * 1000, LocalTime.now().toString().getBytes());

        cf.join();

    }


    static final NSQLookup lookup = new DefaultNSQLookup();

    static {
        lookup.addLookupAddress(Nsq.getNsqLookupdHost(), 4161);
    }

    static NSQProducer newProducer() {
        var p = new NSQProducer();
        var addresses = lookup.lookupNodeAsync().join();
        p.addAddresses(addresses);
        p.start();
        return p;
    }


    public static ExecutorService newBackoffThreadExecutor() {
        return new ThreadPoolExecutor(1, 1,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1));
    }

    private String randomString() {
        return "Message_" + new Date().getTime();
    }


    @Test
    public void testClientCompress() throws IOException, NoConnectionsException, InterruptedException {
        var in = NSQProducerTest.class.getResourceAsStream("/test-data.json");
        var s = STRING_DECODER.apply(in);

        var topic = "test-client-compress";

        var queue = new LinkedBlockingQueue<String>();
        var consumer = new NSQConsumer<>(lookup, topic, "ch", concurrency, (message) -> {
            log.info("Processing message: " + (message.getObj()));
            message.finished();
            try {
                queue.put(message.getObj());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, CONFIG, STRING_DECODER);
        consumer.start();

        {
            pushCompress(topic, s, CompressType.Snappy);
            var m = queue.take();
            Assert.assertEquals(s, m);
        }
        {
            pushCompress(topic, s, CompressType.Deflate);
            var m = queue.take();
            Assert.assertEquals(s, m);
        }

    }

    private void pushCompress(String topic, String data, CompressType compress) throws NoConnectionsException {
        var producer = new NSQProducer();
        var addresses = lookup.lookupNodeAsync().join();
        var config = new NSQConfig();
        config.setCompress(compress);
        producer.addAddresses(addresses);
        producer.setConfig(config);
        producer.start();

        producer.produceAsync(topic, o -> o.write(data.getBytes())).join();
    }

}
