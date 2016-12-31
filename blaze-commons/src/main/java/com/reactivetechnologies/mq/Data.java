package com.reactivetechnologies.mq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public abstract class Data implements DataSerializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String correlationID = "";
	private long timestamp = 0;
	private String destination = "";
	private String replyTo = "";
	private boolean redelivered;
	private long expiryMillis = 0;
	
	@Override
	public void writeData(DataOutput out) throws IOException {
		out.writeUTF(correlationID);
		out.writeUTF(destination);
		out.writeUTF(replyTo);
		out.writeLong(expiryMillis);
		out.writeLong(timestamp);
		out.writeBoolean(redelivered);
	}

	@Override
	public void readData(DataInput in) throws IOException {
		setCorrelationID(in.readUTF());
		setDestination(in.readUTF());
		setReplyTo(in.readUTF());
		setExpiryMillis(in.readLong());
		setTimestamp(in.readLong());
		setRedelivered(in.readBoolean());
	}
		
	public String getCorrelationID() {
		return correlationID;
	}

	public void setCorrelationID(String correlationID) {
		this.correlationID = correlationID;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public String getDestination() {
		return destination;
	}

	public void setDestination(String destination) {
		this.destination = destination;
	}

	public String getReplyTo() {
		return replyTo;
	}

	public void setReplyTo(String replyTo) {
		this.replyTo = replyTo;
	}

	public boolean isRedelivered() {
		return redelivered;
	}

	public void setRedelivered(boolean redelivered) {
		this.redelivered = redelivered;
	}

	public long getExpiryMillis() {
		return expiryMillis;
	}

	public void setExpiryMillis(long expiryMillis) {
		this.expiryMillis = expiryMillis;
	}
	/**
	 * 
	 */
	public Data() {
	}

	

	
}