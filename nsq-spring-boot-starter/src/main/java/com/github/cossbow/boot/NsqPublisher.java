package com.github.cossbow.boot;


import com.github.cossbow.nsq.util.ThrowoutConsumer;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CompletableFuture;

public interface NsqPublisher extends EncoderPublisher {

    Encoder defaultEncoder = NsqUtil.getDefaultEncoder();


    /**
     * 发送推迟[defer]毫秒的消息
     */
    CompletableFuture<Void> publish(String topic, int defer, Object value, Encoder encoder);

    default CompletableFuture<Void> publish(String topic, Object value, Encoder encoder) {
        return publish(topic, 0, value, encoder);
    }

    default CompletableFuture<Void> publish(String topic, int defer, Object value) {
        return publish(topic, defer, value, defaultEncoder);
    }

    default CompletableFuture<Void> publish(String topic, Object value) {
        return publish(topic, 0, value);
    }

    /**
     * 发送推迟[defer]毫秒的消息
     */
    CompletableFuture<Void> publish(String topic, int defer, byte[] value);

    default CompletableFuture<Void> publish(String topic, byte[] value) {
        return publish(topic, 0, value);
    }

    /**
     * 流式
     */
    CompletableFuture<Void> publish(String topic, int defer, ThrowoutConsumer<OutputStream, IOException> callback);

    default CompletableFuture<Void> publish(String topic, ThrowoutConsumer<OutputStream, IOException> callback) {
        return publish(topic, 0, callback);
    }


    /**
     *
     */
    void disconnect();


}
