package com.Rezar.dbSub.base.exceptions;

public class ServerInitException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ServerInitException(String msg) {
		super(msg);
	}

	public ServerInitException(String msg, Exception e) {
		super(msg, e);
	}

}
