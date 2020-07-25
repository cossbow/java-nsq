package com.github.cossbow.boot;

import com.github.cossbow.nsq.NSQMessage;

import java.util.Objects;


public class NsqMessageWrapper<T> {
    private final T value;

    private transient NSQMessage message;

    public NsqMessageWrapper(T value, NSQMessage message) {
        this.value = Objects.requireNonNull(value);
        this.message = Objects.requireNonNull(message);
    }

    public T getValue() {
        return value;
    }


    public void finished() {
        message.finished();
    }

    public void requeue(int timeoutMillis) {
        message.requeue(timeoutMillis);
    }

    public void requeue() {
        message.requeue(0);
    }

    public void touch() {
        message.touch();
    }

    public int getAttempts() {
        return message.getAttempts();
    }

    public NSQMessage getMessage() {
        return message;
    }
}
