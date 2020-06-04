package com.Rezar.dbSub.base.exceptions;

public class TooOldSeqIdException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public final String seqId;

	public TooOldSeqIdException(String seqId, String msg) {
		super(msg);
		this.seqId = seqId;
	}

}
