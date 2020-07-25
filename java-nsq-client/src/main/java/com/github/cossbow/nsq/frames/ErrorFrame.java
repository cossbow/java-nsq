package com.github.cossbow.nsq.frames;

public class ErrorFrame extends NSQFrame {

    public String getErrorMessage() {
        return readData();
    }

    public String toString() {
        return "ERROR: " + getErrorMessage();
    }
}
