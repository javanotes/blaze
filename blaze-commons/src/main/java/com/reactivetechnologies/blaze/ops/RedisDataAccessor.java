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
package com.reactivetechnologies.blaze.ops;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import com.reactivetechnologies.blaze.struct.QRecord;

@Component
public class RedisDataAccessor {

	private static final String RPOPLPUSH_DESTN_SUFFIX = "$INPROC";
	static final String RPOPLPUSH_DESTN_SET = RPOPLPUSH_DESTN_SUFFIX + "-SET";
	public static final String QUEUE_PREFIX = "queues/";
	public static final String QUEUE_PREFIX_PATTERN = QUEUE_PREFIX+"*";
	public static final String STATS_SUFFIX = "$STAT";
	static final String LIST_KEY_JOIN_SEPARATOR = "-";
	static final String INPROC_KEY_JOIN_SEPARATOR = ".";
	
	@Autowired
	private InstanceInitializationService instanceService;
	static final Logger log = LoggerFactory.getLogger(RedisDataAccessor.class);
	/*
	 * Note: On inspecting the Spring template bound*Ops() in Spring code, 
	 * they seem to be plain wrapper classes exposing a subset of operations (restrictive decorator?), 
	 * and passing on the execution to the proxied template class which is a singleton. 
	 * Thus creating large number of such local instances should not be very expensive.
	 * 
	 * This is purely from a theoretical point of view. If profiling suggests otherwise, then caching the
	 * instances can always be considered.
	 */
	@PostConstruct
	private void init()
	{
		instanceService.verifyInstanceId();
		loadQueueNames();
	}
	private void persistQueueName(String name) {
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
	}

	
	@PreDestroy
	private void destroy()
	{
		instanceService.removeInstanceId();
		instanceService.persistQueueNames(findQueueNames());
	}
	
	private void loadQueueNames() {
		Set<String> qNames = findQueueNames();
		queueNames.addAll(qNames);
	}
	/**
	 * Get the current server status info - all.
	 * @return
	 */
	public Properties getServerInfo()
	{
		return redisTemplate.execute(new RedisCallback<Properties>() {

			@Override
			public Properties doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.info("all");
			}
		});
	}
	//TODO: Fetching queue names. Not being able to PERSIST
	/**
	 * Fetch the name of queues being maintained in Redis. Uses the KEYS
	 * command and {@literal QUEUE_PREFIX_PATTERN}, with an O(n) complexity
	 * @return
	 *
	 */
	public Set<String> findQueueNames()
	{
		Set<String> qList = redisTemplate.keys(QUEUE_PREFIX_PATTERN);
		for(Iterator<String> iter = qList.iterator(); iter.hasNext();)
		{
			String qName = iter.next();
			if(qName.contains(RPOPLPUSH_DESTN_SUFFIX) || qName.contains(STATS_SUFFIX))
			{
				iter.remove();
			}
		}
		return qList;
	}
	
	/**
	 * Mark the end of a commit phase. The action would be to remove message from inproc queue.
	 * Also, if enqueueAgain is true, message will be enqueued at head of source queue. The operation
	 * happens atomically from within a MULTI block.
	 * 
	 * @param qr
	 * @param key
	 * @param enqueueAgain
	 */
	public void endCommit(QRecord qr, String key, boolean enqueueAgain) {
		List<Object> results = redisTemplate.execute(new SessionCallback<List<Object>>() {

			@Override
			public <K, V> List<Object> execute(RedisOperations<K, V> operations) throws DataAccessException {
				operations.multi();
				
				//TODO: Verify if this is indeed being executed in a MULTI correctly
				
				//ListOperations<K, V> listOps = operations.opsForList();
				
				//listOps.remove((K) prepareInProcKey(key), -1, qr.getRedeliveryCount() > 0 ? qr.decrDeliveryCount() : qr);
				
				//the QRecord has been incremented now. So to make the 'remove'
				//operation fire, we will decrement the count to make it equal
				//to the state saved in the inproc queue
				redisTemplate.boundListOps(prepareInProcKey(key)).remove(-1,
						qr.getRedeliveryCount() > 0 ? qr.decrDeliveryCount() : qr);
				
				if(enqueueAgain)
				{
					redisTemplate.boundListOps(key).rightPush(qr);
					
					//listOps.rightPush((K) key, (V) qr);
				}
				else
				{
					//dequeue is complete now
					statsRecorder.recordDequeu(key);
				}
				return operations.exec();
			}
			
		});
		//LREM count < 0: Remove elements equal to value moving from tail to head.
		//since we are pushing from left, the item will be moving towards tail. this operation
		//has a complexity of O(N), so we should try to minimize N
		long c = (long) results.get(0);
		if (c != 1) {
			log.warn("Message was not removed from inproc on endCommit. count="+c);
		}
	}
	/**
	 * Enqueue item by head of the SOURCE queue from the destination, for message re-delivery.
	 * @param qr
	 * @param preparedKey
	 * @deprecated
	 */
	@SuppressWarnings("unused")
	private void reEnqueue0(QRecord qr, String preparedKey) {
		BoundListOperations<String, QRecord> listOps = redisTemplate.boundListOps(preparedKey);
		listOps.rightPush(qr);
	}
	/**
	 * Enqueue items by head (left). Along with the destination queue (termed SOURCE), another surrogate queue (termed SINK) is used 
	 * for reliability and guaranteed message delivery. See explanation.
	 * <pre>
	 *       == == == == ==
   Right --> ||	 ||	 ||  || <-- Left
(tail)       == == == == ==      (head)
	 * </pre>
	 * 
	 * We do a LPUSH to enqueue items, which is push from head. So the 'first in' item will always be at tail.
	 * While dequeuing, thus the tail has to be popped by a RPOP. To achieve data safety on dequeue
	 * operation, we would do an atomic RPOPLPUSH (POP from tail of a SOURCE queue and push to head of a SINK queue)
	 * 
	 * @see {@link https://redis.io/commands/rpoplpush#pattern-reliable-queue}
	 * @param exchange
	 * @param key the destination queue
	 * @param values
	 */
	public void enqueue(String preparedKey, QRecord...values) {
		log.debug("enqueue: LPUSH "+preparedKey);
		BoundListOperations<String, QRecord> listOps = redisTemplate.boundListOps(preparedKey);
		long c= listOps.leftPushAll(values);
		statsRecorder.recordEnqueu(preparedKey, c);
	}
	private final Set<String> queueNames = new HashSet<>();
	
	//NOTE: Redis keys are data structure specific. So you cannot use the same key for hash and list.
	/**
	 * Prepare a list key based on the exchange and route information. Presently it is
	 * simply appended.
	 * @param exchange
	 * @param key
	 * @return
	 */
	public String prepareListKey(String exchange, String key)
	{
		String name = new StringBuilder(QUEUE_PREFIX).append(exchange).append(LIST_KEY_JOIN_SEPARATOR).append(key).toString();
		persistQueueName(name);
		return name;
	}
	/**
	 * Prepare the key for the surrogate SINK queue.
	 * @param exchange
	 * @param key
	 * @return
	 */
	public String prepareInProcKey(String exchange, String key)
	{
		String preparedKey = prepareListKey(exchange, key);
		return prepareInProcKey(preparedKey);
	}
	
	/**
	 * 
	 * @param preparedKey
	 * @return
	 */
	private String prepareInProcKey(String preparedKey)
	{
		return preparedKey + RPOPLPUSH_DESTN_SUFFIX + INPROC_KEY_JOIN_SEPARATOR + instanceService.getInstanceId();
	}
	@Autowired StringRedisTemplate stringRedis;
	@Autowired BlazeRedisTemplate redisTemplate;
	@Autowired
	private RedisStatsRecorder statsRecorder;
	/**
	 * RPOP the next available item from SOURCE queue tail, and LPUSH it to a SINK queue head. This is
	 * done to handle message delivery in case of failed attempts.
	 * 
	 * @see https://redis.io/commands/brpoplpush
	 * @param xchng
	 * @param route
	 * @param await
	 * @param unit
	 * @return
	 * @throws TimeoutException
	 */
	public QRecord dequeue(String xchng, String route, long await, TimeUnit unit) 
	{
		log.debug(">>>>>>>>>> Start fetchHead <<<<<<<<< ");
		log.debug("route -> " + route + "\tawait: " + await + " unit: " + unit);
		String preparedKey = prepareListKey(xchng, route);
		String inprocKey = prepareInProcKey(preparedKey);
		
		log.debug("dequeue: RPOP "+preparedKey+" LPUSH "+inprocKey);
		return redisTemplate.opsForList().rightPopAndLeftPush(preparedKey, inprocKey, await, unit);
	}
	/**
	 * RPOP operation. This method should be used in message polling. For a reliable messaging,
	 * like in consumer deliveries, {@link #dequeue()} should be considered.
	 * @param xchng
	 * @param route
	 * @return dequeued item or null
	 */
	public QRecord pop(String xchng, String route, long await, TimeUnit unit)
	{
		String preparedKey = prepareListKey(xchng, route);
		QRecord qr =  redisTemplate.opsForList().rightPop(preparedKey, await, unit);
		if(qr != null)
		{
			statsRecorder.recordDequeu(preparedKey);
		}
		return qr;
		
	}
	
	/**
	 * Wrapper on {@linkplain RedisTemplate}.
	 * @param prepareKey
	 * @return
	 */
	public BoundListOperations<String, QRecord> boundListOps(String prepareKey) {
		return redisTemplate.boundListOps(prepareKey);
	}
	/**
	 * Wrapper on {@linkplain RedisTemplate}.
	 * @param redisCallback
	 */
	public void executePipelined(RedisCallback<Integer> redisCallback) {
		redisTemplate.executePipelined(redisCallback);
	}
	/**
	 * Clears a given queue using a MULTI RPOP iteration.
	 * @param xchangeKey
	 * @param routeKey
	 * @return true if cleared
	 */
	public boolean clear(String xchangeKey, String routeKey)
	{
		final String listKey = prepareListKey(xchangeKey, routeKey);

		//it is better to run clear in pipeline for faster execution
		//anyway we return a boolean to confirm
		//so clients higher up can do a compare and set type operation.
		List<Object> removed = clearPipelined(listKey);
		
		log.debug("Removed items: "+removed);
		log.info("Removed items count: "+removed.size());
		return sizeOf(listKey) == 0;
	}
	
	private List<Object> clearPipelined(final String listKey)
	{
		StringRedisSerializer ser = (StringRedisSerializer) redisTemplate.getKeySerializer();
		final byte[] keyBytes = ser.serialize(listKey);
		final long llen = sizeOf(listKey);
		return redisTemplate.executePipelined(new RedisCallback<Void>() {

			@Override
			public Void doInRedis(RedisConnection connection) throws DataAccessException {
				if (llen > 0) {
					for (long l = 0; l < llen; l++) {
						connection.rPop(keyBytes);
					}
				}
				return null;
			}
		});
	}
	private List<Object> clearTransactional(final String listKey)
	{
		
		return redisTemplate.execute(new SessionCallback<List<Object>>() {

			@SuppressWarnings("unchecked")
			@Override
			public <K, V> List<Object> execute(RedisOperations<K,V> operations) throws DataAccessException {
				try {
					long llen = sizeOf(listKey);
					if (llen > 0) {
						ListOperations<K, V> ops = operations.opsForList();
						operations.multi();
						for (long l = 0; l < llen; l++) {
							ops.rightPop((K) listKey);
						}
						return operations.exec();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				return Collections.emptyList();
			}
		});
	}
	/**
	 * Clear the inproc queue
	 * @param xchangeKey
	 * @param routeKey
	 * @return
	 */
	public boolean clearInproc(String xchangeKey, String routeKey)
	{
		final String listKey = prepareInProcKey(xchangeKey, routeKey);

		List<Object> removed = clearTransactional(listKey);
		log.debug("Removed items: "+removed);
		log.info("Removed items count: "+removed.size());
		return sizeOf(listKey) == 0;
	}
	/**
	 * Wrapper on {@linkplain RedisTemplate}.
	 * @return
	 */
	public StringRedisSerializer getKeySerializer() {
		return (StringRedisSerializer) redisTemplate.getKeySerializer();
	}
	/**
	 * Get LLEN for the queue
	 * @param listKey
	 * @return 
	 */
	public Long sizeOf(String listKey) {
		return redisTemplate.boundListOps(listKey).size();
	}
	/**
	 * Enqueue items from the recovery (inproc) queue. These items will be appended at tail
	 * of the source queue. So it is possible to get a backdated item popped now. This method
	 * is thus opposite to  the {@link #dequeue()} method in action. 
	 * @param xchng
	 * @param route 
	 * @return 
	 */
	public boolean reverseDequeue(String xchng, String route) {
		String preparedKey = prepareListKey(xchng, route);
		String inprocKey = prepareInProcKey(preparedKey);
		log.info("reverseDequeue: RPOP "+inprocKey+" LPUSH "+preparedKey);
		QRecord qr = redisTemplate.opsForList().rightPopAndLeftPush(inprocKey, preparedKey);
		return qr != null;
	}
}
