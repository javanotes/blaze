package com.reactivetechnologies.blaze.struct;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import com.reactivetechnologies.mq.Data;
import com.reactivetechnologies.mq.QueueService;
import com.reactivetechnologies.mq.exceptions.BlazeInternalException;

public class QRecord implements Serializable{

	@Override
	public String toString() {
		return "QRecord [key=" + key + ", replyTo=" + replyTo + ", redelivered=" + redelivered + ", expiryMillis="
				+ expiryMillis + ", corrId=" + corrId + ", t0TS=" + t0TS + ", tnTS=" + tnTS + "]";
	}
	public QRecord(){
		setKey(new QKey());
	}
	/**
	 * Return a copied instance with decremented delivery count. This method is needed to get a 'previous state'
	 * of an incremental {@linkplain QRecord}.
	 * @return
	 */
	public QRecord decrDeliveryCount()
	{
		QRecord copy = new QRecord(this);
		copy.setRedeliveryCount((short) (this.getRedeliveryCount()-1));
		return copy;
	}
	/**
	 * Transform an instance of {@linkplain Data} to a {@linkplain QRecord}.
	 * @param t
	 * @param xchangeKey
	 * @param routeKey
	 * @return
	 */
	public static QRecord transformData(Data t, String xchangeKey, String routeKey)
	{
		return transformData(t, xchangeKey, routeKey, UUID.randomUUID());
	}
	/**
	 * 
	 * @param t
	 * @param xchangeKey
	 * @param routeKey
	 * @param uid
	 * @return
	 */
	public static QRecord transformData(Data t, String xchangeKey, String routeKey, UUID uid)
	{
		QRecord qr = new QRecord(t);
		qr.getKey().setExchange(xchangeKey);
		qr.getKey().setRoutingKey(routeKey);
		qr.getKey().setTimeuid(uid);
		qr.setT0TS(new Date());
		
		return qr;
	}
	public static QRecord transformData(Data textData) {
		return transformData(textData, QueueService.DEFAULT_XCHANGE, textData.getDestination());
	}
	private QRecord(QRecord q)
	{
		this.setKey(q.getKey());
		this.setCorrId(q.getCorrId());
		this.setExpiryMillis(q.getExpiryMillis());
		this.setPayload(q.getPayload());
		this.setRedelivered(q.isRedelivered());
		this.setRedeliveryCount(q.getRedeliveryCount());
		this.setReplyTo(q.getReplyTo());
		this.setT0TS(q.getT0TS());
		this.setTnTS(getTnTS());
	}
	public QRecord(Data md)
	{
		this();
		setKey(new QKey(md.getDestination()));
		if (md.getCorrelationID() != null) {
			setCorrId(md.getCorrelationID());
		}
		setReplyTo(md.getReplyTo());
		setExpiryMillis(md.getExpiryMillis());
		setRedelivered(md.isRedelivered());
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			md.writeData(new DataOutputStream(bos));
		} catch (IOException e) {
			throw new BlazeInternalException("Unable to serialize message", e);
		}
		setPayload(ByteBuffer.wrap(bos.toByteArray()));
	}
	private AtomicInteger redeliveryCount = new AtomicInteger();
	/**
	 * 
	 */
	private static final long serialVersionUID = 8567354805104926577L;
	public QKey getKey() {
		return key;
	}
	public void setKey(QKey key) {
		this.key = key;
	}
	
	public ByteBuffer getPayload() {
		return payload;
	}
	public void setPayload(ByteBuffer payload) {
		this.payload = payload;
	}
	public String getCorrId() {
		return corrId;
	}
	public void setCorrId(String corrId) {
		this.corrId = corrId;
	}
	private QKey key;
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
	private String replyTo = "";
	private boolean redelivered;
	private long expiryMillis = 0;
	private ByteBuffer payload;
	private String corrId = "";
	private Date t0TS;
	public Date getT0TS() {
		return t0TS;
	}
	public void setT0TS(Date t0ts) {
		t0TS = t0ts;
	}
	public Date getTnTS() {
		return tnTS;
	}
	public void setTnTS(Date tnTS) {
		this.tnTS = tnTS;
	}
	private Date tnTS;
	
	public short getRedeliveryCount() {
		return redeliveryCount.shortValue();
	}
	public void setRedeliveryCount(short redeliveryCount) {
		this.redeliveryCount.set(redeliveryCount);
	}
	public void incrDeliveryCount() {
		this.redeliveryCount.incrementAndGet();
	}

	public boolean isExpired() {
		return expiryMillis <= 0 ? false
				: getT0TS() != null ? System.currentTimeMillis() - getT0TS().getTime() > getExpiryMillis() : false;
	}
	
}
