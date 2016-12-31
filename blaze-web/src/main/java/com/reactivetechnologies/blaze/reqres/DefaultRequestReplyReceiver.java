/**
 * Copyright 2016 esutdal

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
package com.reactivetechnologies.blaze.reqres;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.reactivetechnologies.mq.Data;
import com.reactivetechnologies.mq.consume.Consumer;
import com.reactivetechnologies.mq.consume.QueueListener;
import com.reactivetechnologies.mq.consume.QueueListenerBuilder;
import com.reactivetechnologies.mq.container.QueueContainer;
/**
 * Asynchronous reply queue listener. This component should be used to query for a response {@linkplain Data} in a request-reply
 * semantic. Internally uses {@linkplain ResultCache} to cache the response atomically.
 * @author esutdal
 *
 */
//TODO: WIP
public class DefaultRequestReplyReceiver implements RequestReplyReceiver {

	private QueueContainer container;
	private ResultCache resultCache;
	/**
	 * The reply listener class.
	 * @author esutdal
	 *
	 */
	private final class ReplyListener implements Consumer<Data> {
		
		@Override
		public void onMessage(Data m) throws Exception 
		{
			resultCache.set(m);
			
		}

		@Override
		public void destroy() {
			try {
				resultCache.close();
			} catch (IOException e) {
				log.warn("", e);
			}
		}
		@Override
		public void init() {
			resultCache = new ResultCache();
		}
	}
	/**
	 * The common reply queue, for all request-response invocations.
	 * 
	 */
	public static final String ASYNC_REPLYQ = "COMMON_REPLYQ";
	private QueueListener<Data> listener;
	private static final Logger log = LoggerFactory.getLogger(DefaultRequestReplyReceiver.class);

	/**
	 * 
	 * @param container
	 */
	public DefaultRequestReplyReceiver(QueueContainer container) {
		this.container = container;
	}
	
	/* (non-Javadoc)
	 * @see com.blaze.mq.redis.callback.IAsyncReplyReceiver#awaitAndGet(java.lang.String, long, java.util.concurrent.TimeUnit)
	 */
	@Override
	public Data awaitAndGet(String corrId, long maxWait, TimeUnit unit)
	{
		Assert.notNull(corrId);
		Assert.isTrue(maxWait > 0, "Expectings maxWait > 0");
		try {
			return resultCache.get(corrId, maxWait, unit);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return null;
	}
	/* (non-Javadoc)
	 * @see com.blaze.mq.redis.callback.IAsyncReplyReceiver#get(java.lang.String)
	 */
	@Override
	public Data get(String corrId) throws InterruptedException
	{
		Assert.notNull(corrId);
		return resultCache.get(corrId, 0, TimeUnit.MILLISECONDS);
	}
	/* (non-Javadoc)
	 * @see com.blaze.mq.redis.callback.IAsyncReplyReceiver#getIfPresent(java.lang.String)
	 */
	@Override
	public Data getIfPresent(String corrId)
	{
		return resultCache.getNow(corrId);
	}
			
	@PostConstruct
	private void init()
	{
		listener = new QueueListenerBuilder()
		.concurrency(Runtime.getRuntime().availableProcessors())
		.consumer(new ReplyListener())
		.sharedPool(false)
		.identifier("AsyncListener")
		.route(ASYNC_REPLYQ)
		.dataType(Data.class)
		.build();
		
		container.register(listener);
		
		log.info("Async reply listener started ..");
	}

	@Override
	public void put(Data d) {
		resultCache.set(d);
	}

}
