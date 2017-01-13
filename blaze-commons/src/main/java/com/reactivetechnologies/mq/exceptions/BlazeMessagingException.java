package com.reactivetechnologies.mq.exceptions;

import com.reactivetechnologies.mq.Data;

public class BlazeMessagingException extends Exception {

	private Data record;
	public BlazeMessagingException(String msg, Throwable cause) {
		super(msg, cause);
	}
	public BlazeMessagingException(Throwable cause) {
		super("", cause);
	}
	public Data getRecord() {
		return record;
	}
	public void setRecord(Data record) {
		this.record = record;
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
