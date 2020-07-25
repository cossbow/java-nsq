package com.github.cossbow.boot;

import com.github.cossbow.nsq.NSQConfig;
import com.github.cossbow.nsq.NSQProducer;
import com.github.cossbow.nsq.exceptions.NoConnectionsException;
import com.github.cossbow.nsq.lookup.NSQLookup;
import com.github.cossbow.nsq.util.ThrowoutConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;


class NsqPublisherWrapper implements NsqPublisher {
    private static final Logger log = LoggerFactory.getLogger(NsqPublisherWrapper.class);

    private final NSQLookup nsqLookup;

    private final NSQProducer producer;

    NsqPublisherWrapper(NSQLookup nsqLookup) {
        this(nsqLookup, null);
    }

    NsqPublisherWrapper(NSQLookup nsqLookup, NSQConfig config) {
        this.nsqLookup = nsqLookup;

        this.producer = new NSQProducer();
        if (null != config) {
            this.producer.setConfig(config);
        }
        //
        this.nsqLookup.lookupNodeAsync().thenAccept((list) -> {
            this.producer.addAddresses(list);
            this.producer.start();
        }).join();
    }


    //

    private OutputStream messageStream(OutputStream os) {
        return os;
    }

    @Override
    public CompletableFuture<Void> publish(String topic, int defer, Object value, Encoder encoder) {
        if (null == value) {
            log.error("topic[{}] got null value", topic);
            return CompletableFuture.completedFuture(null);
        }

        try {
            return producer.produceAsync(topic, defer, os -> {
                try (var out = messageStream(os)) {
                    encoder.encode(out, value);
                }
            });
        } catch (NoConnectionsException e) {
            return CompletableFuture.failedFuture(new NsqException("publish: no connection alive", e));
        }

    }

    @Override
    public CompletableFuture<Void> publish(String topic, int defer, byte[] value) {
        if (null == value) {
            log.error("topic[{}] got null value", topic);
            return CompletableFuture.completedFuture(null);
        }

        try {
            return producer.produceAsync(topic, defer, os -> {
                try (var out = messageStream(os)) {
                    out.write(value);
                }
            });
        } catch (NoConnectionsException e) {
            throw new NsqException("publish: no connection alive", e);
        }
    }


    @Override
    public CompletableFuture<Void> publish(String topic, int defer, ThrowoutConsumer<OutputStream, IOException> callback) {
        if (null == callback) {
            log.error("topic[{}] got null value", topic);
            return CompletableFuture.completedFuture(null);
        }

        try {
            return producer.produceAsync(topic, defer, os -> {
                try (var out = messageStream(os)) {
                    callback.accept(out);
                }
            });
        } catch (NoConnectionsException e) {
            throw new NsqException("publish: no connection alive", e);
        }
    }


    @Override
    public void disconnect() {
        if (null != producer) {
            producer.shutdown();
        }
    }

}
