package com.github.cossbow.nsq.exceptions;

public class DisconnectedException extends NSQException {
	private static final long serialVersionUID = -7019786584295718137L;

	public DisconnectedException(String message, Throwable cause) {
		super(message, cause);
	}
}
