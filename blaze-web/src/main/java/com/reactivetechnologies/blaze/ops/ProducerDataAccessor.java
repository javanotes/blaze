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
package com.reactivetechnologies.blaze.ops;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.stereotype.Component;

import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.mq.exceptions.RedisUnavailableException;
import com.reactivetechnologies.mq.ops.ProducerOperations;
@Component
public class ProducerDataAccessor extends BaseDataAccessor implements ProducerOperations {

	private final AtomicBoolean initReady = new AtomicBoolean();
	private static final Logger log = LoggerFactory.getLogger(ProducerDataAccessor.class);
	
	@Value("${producer.connChecker.period.millis:5000}")
	private long connCheckPeriodMillis = 5;
	
	@Value("${producer.connChecker.rejectOnUnavailable:true}")
	private boolean isRejectOnRedisDown;
		
	private final Set<String> queueNames = new HashSet<>();
	@Autowired
	private LocalDataAccessorFactoryBean localDataFactory;
	
	@PreDestroy
	private void destroy()
	{
		super.doDestroy();
		if(localData.isOpen())
			try {
				localData.close();
			} catch (IOException e) {
				
			}
	}
	
	@PostConstruct
	private void init()
	{
		if(isRedisAvailable())
		{
			log.info("Verified Redis is available..");
			localData = localDataFactory.getObject();
			doInit();
		}
		else
		{
			scheduleConnectionCheck();
		}
	}
	
	private LocalDataAccessor localData;
	/**
	 * Connection checker.
	 * @author esutdal
	 *
	 */
	private final class ConnectionCheckerTask implements Runnable
	{
		private ScheduledExecutorService connChecker;
		private final long period;
		private TimeUnit unit = TimeUnit.MILLISECONDS;
		public ConnectionCheckerTask(long period) {
			this.period = period;
			connChecker = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
				
				@Override
				public Thread newThread(Runnable arg0) {
					Thread t = new Thread(arg0, "Blaze.ConnectionChecker");
					t.setDaemon(true);
					return t;
				}
			});
			log.info("Initiated new connection checker task..");
		}
		@Override
		public void run() {
			if(isRedisAvailable())
			{
				log.info("Redis is up and available now. Running initializations..");
				doInit();
				connChecker.shutdown();
			}
			else
			{
				log.warn("Scheduling next connection check run");
				scheduleNextRun();
			}
		}
		private void scheduleNextRun()
		{
			connChecker.schedule(this, period, unit);
		}
	}
	private void loadQueueNames() {
		Set<String> qNames = findQueueNames();
		queueNames.addAll(qNames);
	}
	protected void doInit()
	{
		super.doInit();
		loadQueueNames();
		initReady.compareAndSet(false, true);
		moveLocal();
	}
	private void moveLocal() {
		localData.moveAll(this);
		log.info("Local queue/s moved to Redis, or removed");
	}
	private void scheduleConnectionCheck()
	{
		localData = localDataFactory.getObject();
		new ConnectionCheckerTask(connCheckPeriodMillis).scheduleNextRun();
	}
	@Override
	public void enqueue(String preparedKey, QRecord... values) {
		log.debug("enqueue: LPUSH "+preparedKey);
		try 
		{
			if (initReady.get()) {
				lpushAll(preparedKey, values);
			}
			else
			{
				enqueueLocally(preparedKey, values);
			}
		} 
		catch (RedisConnectionFailureException e) {
			log.error("", e);
			if(initReady.compareAndSet(true, false))
				scheduleConnectionCheck();
			
			enqueueLocally(preparedKey, values);
			//handle again?
		}
	}

	private void enqueueLocally(String preparedKey, QRecord[] values) {
		if(isRejectOnRedisDown)
			throw new RedisUnavailableException();
		
		log.warn("Redis is unavailable. Will queue it locally");
		localData.addAll(preparedKey, values);
	}
	/*
	 * (non-Javadoc)
	 * @see com.reactivetechnologies.mq.ops.ProducerOperations#lpushAll(java.lang.String, com.reactivetechnologies.blaze.struct.QRecord[])
	 */
	@Override
	public void lpushAll(String preparedKey, QRecord[] values)
	{
		BoundListOperations<String, QRecord> listOps = redisTemplate.boundListOps(preparedKey);
		long c = listOps.leftPushAll(values);
		statsRecorder.recordEnqueu(preparedKey, c);
	}
	//NOTE: Redis keys are data structure specific. So you cannot use the same key for hash and list.
	/*
	 * (non-Javadoc)
	 * @see com.reactivetechnologies.blaze.ops.BaseDataAccessor#prepareListKey(java.lang.String, java.lang.String)
	 */
	@Override
	public String prepareListKey(String exchange, String key)
	{
		String name = new StringBuilder(QUEUE_PREFIX).append(exchange).append(LIST_KEY_JOIN_SEPARATOR).append(key).toString();
		//persistQueueName(name);
		return name;
	}
	
	/*private void persistQueueName(String name) {
	if(!queueNames.contains(name))
	{
		synchronized (queueNames) {
			if(!queueNames.contains(name))
			{
				instanceService.persistListKeys(new HashSet<>(Arrays.asList(name)));
				queueNames.add(name);
			}
		}
	}
	}*/
}
