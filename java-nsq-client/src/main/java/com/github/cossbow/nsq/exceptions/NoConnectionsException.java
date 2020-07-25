package com.github.cossbow.nsq.exceptions;

public class NoConnectionsException extends NSQException {
	private static final long serialVersionUID = 6007840818432224700L;

	public NoConnectionsException(String message) {
		super(message);
	}

	public NoConnectionsException(String message, Throwable cause) {
		super(message, cause);
	}
}
