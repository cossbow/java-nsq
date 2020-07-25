package com.github.cossbow.boot;

import com.google.gson.Gson;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {NsqAutoConfiguration.class})
public class NsqSnappyTest extends NsqBaseTest {
    static {
        System.setProperty("mq.nsq.snappy", "true");
    }

    @Autowired
    NsqPublisher publisher;

    @Autowired
    @Qualifier(NsqUtil.QUALIFIER_PUB_SNAPPY)
    private NsqPublisher snappyPublisher;

    @Autowired
    NsqSubscriber subscriber;

    private final Serializer serializer;

    {
        serializer = new GsonSerializer(new Gson());
    }

    private News create() {
        return create(ThreadLocalRandom.current().nextInt(1000, 99999));
    }

    @Test
    public void testCoder() throws InterruptedException {
        var syncer = new Object();

        var topic = "test-snappy-coder-1";
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

        var topic = "test-snappy-coder-1";
        subscriber.subscribe(topic, "ch", News.class, news -> {
            System.out.println(news);
            synchronized (syncer) {
                syncer.notifyAll();
            }
        });

        snappyPublisher.publish(topic, create()).join();

        synchronized (syncer) {
            syncer.wait();
        }
    }

    @Test
    public void testBytes() throws InterruptedException {
        var syncer = new Object();

        var topic = "test-snappy-bytes-1";
        subscriber.subscribe(topic, "ch", NsqUtil.STRING_DECODER, s -> {
            System.out.println(s);
            synchronized (syncer) {
                syncer.notifyAll();
            }
        });

        var v = create().toString().getBytes();
        publisher.publish(topic, v).join();

        synchronized (syncer) {
            syncer.wait();
        }
    }

    @Test
    public void testCrossSubscribe() throws InterruptedException {
        var syncer = new CountDownLatch(1);

        var topic = "test-snappy-cross-1";
        subscriber.subscribe(topic, "ch", News.class, serializer, s -> {
            System.out.println(s);
            syncer.countDown();
        });

        syncer.await();
    }

}
