/* ============================================================================
*
* FILE: AbstractQueueListener.java
*
The MIT License (MIT)

Copyright (c) 2016 Sutanu Dalui

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*
* ============================================================================
*/
package com.reactivetechnologies.mq.consume;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.mq.Data;
import com.reactivetechnologies.mq.DataSerializable;
import com.reactivetechnologies.mq.QueueService;
import com.reactivetechnologies.mq.common.BlazeInternalError;
import com.reactivetechnologies.mq.common.BlazeMessagingException;
import com.reactivetechnologies.mq.container.QueueContainer;

/**
 * Abstract base class to be extended for registering queue listeners.
 * @see QueueContainer#register(QueueListener)
 */
public abstract class AbstractQueueListener<T extends Data> implements QueueListener<T>{
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public final int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((identifier() == null) ? 0 : identifier().hashCode());
		return result;
	}
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return identifier()+" [exchange()=" + exchange() + ", routing()=" + routing() + ", concurrency()=" + concurrency() + "]";
	}
	/*
	 * (non-Javadoc)
	 * @see com.blaze.mq.consume.Consumer#destroy()
	 */
	@Override
	public void destroy() {
		//noop
	}
	/**
	 * Return false if a dedicated thread pool is requested for this consumer. When using a
	 * dedicated pool, the worker thread is not configurable. It will kept equal to the number of cores available.
	 * The consumer concurrency however, can always be configured. This is done to limit the number of threads created
	 * with an application creating arbitrary number of consumers. 
	 * @return Whether to use the shared pool. Default true.
	 */
	public boolean useSharedPool()
	{
		return true;
	}
	private static final Logger log = LoggerFactory.getLogger(AbstractQueueListener.class);
	/*
	 * (non-Javadoc)
	 * @see com.blaze.mq.consume.QueueListener#onExceptionCaught(java.lang.Throwable, com.blaze.mq.Data)
	 */
	@Override
	public void onExceptionCaught(Throwable e, Data d)
	{
		log.debug("Execution exception handler. May be overriden by listener implementations. "
				+ "Check {AbstractQueueListener#onExceptionCaught(Throwable, Data)}");
	}
	/**
	 * Override to provide a different exchange.
	 * @return
	 */
	@Override
	public String exchange()
	{
		return  QueueService.DEFAULT_XCHANGE;
	}
	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public final boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		@SuppressWarnings("unchecked")
		QueueListener<T> other = (QueueListener<T>) obj;
		if (identifier() == null) {
			if (other.identifier() != null)
				return false;
		} else if (!identifier().equals(other.identifier()))
			return false;
		return true;
	}
	
	/**
	 * To be overridden to provide a listener identifier.
	 * 
	 * @return
	 */
	public String identifier() {
		return UUID.randomUUID() + "";
	}

	/**
	 * To be overridden to increase concurrency.
	 * 
	 * @return
	 */
	public int concurrency() {
		return 1;
	}
	@Override
	public short maxDeliveryAttempts(){
		return 3;
	}
	static boolean isTimeUid(UUID u)
	{
		if(u == null)
			return false;
		try {
			u.timestamp();
		} catch (UnsupportedOperationException e) {
			return false;
		}
		return true;
	}
	/**
	 * Used internally.
	 * 
	 * @param arg0
	 * @param arg1
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 * @throws IOException 
	 * @throws BlazeMessagingException 
	 * @throws Exception
	 */
	public final void fireOnMessage(QRecord obs) throws BlazeMessagingException  {
		try 
		{
			T obj = dataType().newInstance();
			readData(obs.getPayload(), obj);
			obj.setCorrelationID(obs.getCorrId());
			obj.setDestination(exchange()+"."+routing());
			obj.setExpiryMillis(obs.getExpiryMillis());
			obj.setRedelivered(obs.isRedelivered());
			obj.setReplyTo(obs.getReplyTo());
			obj.setTimestamp(obs.getT0TS().getTime());
			
			try {
				onMessage(obj);
			} catch (Exception e) {
				BlazeMessagingException ce = new BlazeMessagingException(e);
				ce.setRecord(obj);
				throw ce;
			}
		} catch (InstantiationException | IllegalAccessException | IOException e) {
			throw new BlazeInternalError("Fatal error", e);
		}
	}
	/**
	 * Deserialize the payload bytes to an instance of {@linkplain DataSerializable}.
	 * @param b
	 * @param obj
	 * @throws IOException
	 */
	protected void readData(ByteBuffer b, T obj) throws IOException
	{
		byte[] bytes;
		if(b.hasArray())
		{
			bytes = b.array();
		}
		else
		{
			bytes = new byte[b.limit()];
			b.get(bytes);
		}
		obj.readData(new DataInputStream(new ByteArrayInputStream(bytes)));
	}
	/*
	 * (non-Javadoc)
	 * @see com.reactivetech.messaging.api.QueueListener#allowRedelivery(boolean, short, com.reactivetech.messaging.api.Data). 
	 */
	/**
	 * Will default to checking the expired flag and delivery count. Override this method in order
	 * to inspect the message {@linkplain Data} for advanced retry strategy.
	 */
	@Override
	public boolean allowRedelivery(boolean expired, short redeliveryCount, Data d) {
		if (log.isDebugEnabled()) {
			log.debug("IN allowRedelivery for message " + d);
			log.debug("expired ? " + expired);
			log.debug("delivery count => " + redeliveryCount);
			log.debug("maxDeliveryAttempts => " + maxDeliveryAttempts());
		}
		return !expired && redeliveryCount < maxDeliveryAttempts();
	}

}
