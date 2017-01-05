/**
 * Copyright 2017 esutdal

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.reactivetechnologies.blaze.core;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactivetechnologies.blaze.ops.RedisDataAccessor;
import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.blaze.throttle.ConsumerThrottler;
/**
 * An iterator for fetching the queue head, based on throttling.
 * @author esutdal
 *
 */
class BlazeQueueIterator implements Iterator<QRecord>{

	private static final Logger log = LoggerFactory.getLogger(BlazeQueueIterator.class);
	/**
	 * 
	 * @param throttler
	 * @param throttleTps
	 * @param redisOps
	 */
	public BlazeQueueIterator(ConsumerThrottler throttler, int throttleTps, RedisDataAccessor redisOps) {
		super();
		this.throttler = throttler;
		this.throttleTps = throttleTps;
		this.redisOps = redisOps;
	}
	private final ConsumerThrottler throttler;
	private int throttleTps;
	private final RedisDataAccessor redisOps;
	private String routing;
	public String getRouting() {
		return routing;
	}
	public void setRouting(String routing) {
		this.routing = routing;
	}
	public String getExchange() {
		return exchange;
	}
	public void setExchange(String exchange) {
		this.exchange = exchange;
	}
	public long getPollIntervalMillis() {
		return pollIntervalMillis;
	}
	public void setPollIntervalMillis(long pollIntervalMillis) {
		this.pollIntervalMillis = pollIntervalMillis;
	}
	private String exchange;
	private long pollIntervalMillis;
	
	/**
	 * Returns the next element in the iteration. The queue head in this case.
	 * It is important to note that this method will <b>never</b> throw {@linkplain NoSuchElementException},
	 * due to the fact that {@link #hasNext()} is decided based on throttling configurations.
	 * @see #hasNext()
	 */
	@Override
	public QRecord next() 
	{
		return fetchAndIncrement(exchange, routing, pollIntervalMillis);
		
	}
	/**
	 * Returns true if the message consumption is not to be throttled. Will verify by invoking {@linkplain ConsumerThrottler#allowMessageConsume()}
	 * for the particular consumer in execution.
	 */
	@Override
	public boolean hasNext()
	{
		return throttler.allowMessageConsume(throttleTps);
	}
	private QRecord fetchAndIncrement(String exchange, String routing, long pollInterval) {
		log.debug("Allowed fetching head");
		QRecord qr = fetchHead(exchange, routing, pollInterval);
		log.debug("Incrementing count");
		throttler.incrementCount();
		log.debug("Returning..");
		return qr;
	}
	private QRecord fetchHead(String exchange, String routing, long pollInterval) {
		QRecord qr = redisOps.dequeue(exchange, routing,
				pollInterval, TimeUnit.MILLISECONDS);
		
		return qr;
	}
		
}
