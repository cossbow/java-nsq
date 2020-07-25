package com.github.cossbow.nsq.exceptions;

import com.github.cossbow.nsq.frames.ErrorFrame;

public class NSQException extends Exception {
    private static final long serialVersionUID = 4586554564322836118L;

    public NSQException(String message) {
        super(message);
    }

    public NSQException(String message, Throwable cause) {
        super(message, cause);
    }

    public static NSQException of(ErrorFrame frame) {
        String err = frame.getErrorMessage();
        if (err.startsWith("E_BAD_TOPIC")) {
            return new BadTopicException(err);
        }
        if (err.startsWith("E_BAD_MESSAGE")) {
            return new BadMessageException(err);
        }
        return new NSQException(err);
    }
}
