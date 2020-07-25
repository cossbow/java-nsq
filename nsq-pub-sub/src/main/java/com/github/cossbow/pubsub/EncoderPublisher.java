package com.github.cossbow.pubsub;



import java.util.concurrent.CompletableFuture;

public interface EncoderPublisher {

    CompletableFuture<Void> publish(String topic, int defer, Object value);

    default CompletableFuture<Void> publish(String topic, Object value) {
        return publish(topic, 0, value);
    }


    //
    //
    //

    static EncoderPublisher wrapEncoder(NsqPublisher publisher, Encoder encoder) {
        return (topic, defer, value) -> publisher.publish(topic, defer, value, encoder);
    }

}
