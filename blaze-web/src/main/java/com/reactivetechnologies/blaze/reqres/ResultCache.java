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

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.reactivetechnologies.mq.Data;
/**
 * Reply cache offering set/get operations. The get operations can block. Internally uses {@linkplain ConcurrentHashMap} for concurrent operations.
 * @author esutdal
 *
 */
public class ResultCache implements Closeable{
	
	private DiskCacheLoader diskCache = null;
	public ResultCache() {
		
	}
	/**
	 * If enabled, will flush unconsumed {@linkplain Data} to disk on closing. Default disabled.
	 * @param dir
	 * @param file
	 */
	void enableDiskFlush(String dir, String file)
	{
		diskCache = new DiskCacheLoader(dir, file);
		diskCache.loadFromDisk();
	}
	/**
	 * The reply listener class.
	 * @author esutdal
	 *
	 */

	/**
	 * Put data to the reply cache atomically.
	 * @param m
	 * @param corrId
	 * @param o
	 */
	private void putToCache(Data m, String corrId, Object o)
	{
		try 
		{

			if ((resultCache.putIfAbsent(corrId, m)) != null) 
			{
				//there is a thread waiting for notification which has set the dummy data
				boolean replaced = resultCache.replace(corrId, dummyData, m);
				if (!replaced) {
					synchronized (o) {
						//the put will always be true, since the waiting thread will enter the monitor only if it 
						//has successfully removed the dummy data
						Data d = resultCache.putIfAbsent(corrId, m);
						Assert.isNull(d, "Dummy was not removed when result is placed");
						o.notify();
					}
				}
			}

		} finally {
			waitMap.remove(corrId, o);
		}
	}
	
	private Object initSynchronized(String corrId)
	{
		waitMap.putIfAbsent(corrId, new Object());
		return waitMap.get(corrId);
	}
	/**
	 * Cache set operation.
	 * @param m
	 * @throws Exception
	 */
	public void set(Data m) 
	{
		if(destroyed.compareAndSet(false, false)) 
		{
			String corrId = m.getCorrelationID();
			Object o = initSynchronized(corrId);
			
			putToCache(m, corrId, o);
		}
		else
		{
			if (diskCache != null) {
				log.warn("Listener is shutting down. Dumping message with corrId => " + m.getCorrelationID());
				diskCache.dumpToDisk(m);
			}
		}
		
	}

	private static final Logger log = LoggerFactory.getLogger(DefaultRequestReplyReceiver.class);
		
	private ConcurrentMap<String, Object> waitMap = new ConcurrentHashMap<>();
	private ConcurrentMap<String, Data> resultCache = new ConcurrentHashMap<>();
	
	private final Data dummyData = new Data() {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
	};
	/**
	 * Await and get.
	 * @param corrId
	 * @param maxWait
	 * @param unit
	 * @return The reply {@linkplain Data} if available, or null.
	 * @throws InterruptedException 
	 */
	public Data get(String corrId, long maxWait, TimeUnit unit) throws InterruptedException
	{
		Assert.isTrue(destroyed.compareAndSet(false, false), "Rejected on shutting down");
		return get0(corrId, maxWait, unit);
	}
	/**
	 * Get and returns immediately.
	 * @param corrId
	 * @return The reply {@linkplain Data} if available, or null.
	 */
	public Data getNow(String corrId)
	{
		Assert.isTrue(destroyed.compareAndSet(false, false), "Rejected on shutting down");
		try {
			return get0(corrId, -1, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		return null;
	}
	/**
	 * If maxWait = 0, wait indefinitely; If maxWait > 0, wait for specified time period; 
	 * @param corrId
	 * @param maxWait
	 * @param unit
	 * @throws InterruptedException
	 */
	private void awaitOnGet(String corrId, long maxWait, TimeUnit unit) throws InterruptedException
	{
		//result has not been put, and the dummy data has been removed now
		waitMap.putIfAbsent(corrId, new Object());
		Object o = waitMap.get(corrId);
		synchronized (o) {
			//wait for some time, and if possible get notified
			if (!resultCache.containsKey(corrId)) {
				if (maxWait > 0) {
					o.wait(unit.toMillis(maxWait));
				}
				else
				{
					o.wait();
				}
			}
		}
	}
	/**
	 * Await and get atomically. If maxWait = 0, wait indefinitely; If maxWait > 0, wait for specified time period; 
	 * If maxWait < 0, return immediately.
	 * @param corrId
	 * @param maxWait
	 * @param unit
	 * @return
	 * @throws InterruptedException
	 */
	private Data getAtomic(String corrId, long maxWait, TimeUnit unit) throws InterruptedException
	{
		//if remove is successful that means result has not been put yet
		boolean dummyWasRemoved = resultCache.remove(corrId, dummyData);
		if(dummyWasRemoved)
		{
			if (maxWait >= 0) {
				awaitOnGet(corrId, maxWait, unit); 
			}
		}
		
		return resultCache.get(corrId);
	}
	/**
	 * Await and get result from result cache atomically. Uses {@linkplain ConcurrentMap} atomic
	 * operations. 
	 * @param corrId correlation Id
	 * @param maxWait maximum wait
	 * @param unit unit
	 * @return The reply {@linkplain Data} if available, or null.
	 * @throws InterruptedException
	 */
	private Data get0(String corrId, long maxWait, TimeUnit unit) throws InterruptedException
	{
		Data d = resultCache.putIfAbsent(corrId, dummyData);
		
		//putIfAbsent succeeded. so the result was not yet put in cache.
		if(d == null)
		{
			d = getAtomic(corrId, maxWait, unit);
		}
		
		if(d == null)
			return null;
		
		//result is present in cache. now remove it from cache and return.
		boolean removed = resultCache.remove(corrId, d);
		if(!removed){
			log.debug("Removed was false for corrId => "+corrId+". Returning null;");
		}
		
		return d;
	}
	private AtomicBoolean destroyed = new AtomicBoolean();
	
	private void dumpResultCache()
	{
		while(!destroyed.compareAndSet(true, true));
		if(!resultCache.isEmpty())
		{
			for(Data d : resultCache.values())
			{
				if(d != dummyData)
					diskCache.dumpToDisk(d);
			}
		}
	}

	@Override
	public void close() throws IOException {
		destroyed.compareAndSet(false, true);
		if (diskCache != null) {
			dumpResultCache();
			diskCache.close();
		}
		
	}
	
}
