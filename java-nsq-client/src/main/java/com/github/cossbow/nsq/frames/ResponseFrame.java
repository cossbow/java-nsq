package com.github.cossbow.nsq.frames;

public class ResponseFrame extends NSQFrame {

    public String getMessage() {
        return readData();
    }

    public String toString() {
        return "RESPONSE: " + this.getMessage();
    }
}
