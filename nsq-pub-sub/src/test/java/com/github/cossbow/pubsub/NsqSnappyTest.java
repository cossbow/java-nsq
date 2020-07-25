package com.github.cossbow.pubsub;

import com.github.cossbow.nsq.CompressType;
import com.google.gson.Gson;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;


public class NsqSnappyTest extends NsqBaseTest {
    static {
        System.setProperty("mq.nsq.snappy", "true");
    }


    NsqPublisher publisher = nsqPublisher();

    NsqPublisher snappyPublisher = nsqPublisher(CompressType.Snappy);

    NsqPublisher deflatePublisher = nsqPublisher(CompressType.Deflate);

    NsqSubscriber subscriber = nsqSubscriber();

    private final Serializer serializer;

    {
        serializer = new GsonSerializer(new Gson());
    }

    private News create() {
        return create(ThreadLocalRandom.current().nextInt(1000, 99999));
    }

    @Test
    public void testNon() throws InterruptedException {
        var syncer = new Object();

        var topic = "test-compress-non-1";
        subscriber.subscribe(topic, "ch", News.class, news -> {
            System.out.println(news);
            synchronized (syncer) {
                syncer.notifyAll();
            }
        });

        publisher.publish(topic, create()).join();

        synchronized (syncer) {
            syncer.wait();
        }
    }

    @Test
    public void testSnappy() throws InterruptedException {
        var syncer = new Object();

        var topic = "test-compress-snappy-1";
        subscriber.subscribe(topic, "ch", News.class, news -> {
            System.out.println(news);
            synchronized (syncer) {
                syncer.notifyAll();
            }
        });

        snappyPublisher.publish(topic, create());

        synchronized (syncer) {
            syncer.wait();
        }
    }

    @Test
    public void testDeflate() throws InterruptedException {
        var syncer = new Object();

        var topic = "test-compress-deflate-1";
        subscriber.subscribe(topic, "ch", News.class, news -> {
            System.out.println(news);
            synchronized (syncer) {
                syncer.notifyAll();
            }
        });

        deflatePublisher.publish(topic, create());

        synchronized (syncer) {
            syncer.wait();
        }
    }


}
