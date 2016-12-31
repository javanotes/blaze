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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.data.redis.core.BoundSetOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Component;

import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.mq.common.BlazeDuplicateInstanceException;

@Component
public class RedisDataAccessor {

	private static final String RPOPLPUSH_DESTN_SUFFIX = "$INPROC";
	private static final String RPOPLPUSH_DESTN_SET = RPOPLPUSH_DESTN_SUFFIX + "-SET";
	
	@Value("${blaze.instance.id}")
	private String instanceId;
	@Value("${blaze.instance.id.force:false}")
	private boolean forceApply;
	
	public String getInstanceId() {
		return instanceId;
	}
	private static final Logger log = LoggerFactory.getLogger(RedisDataAccessor.class);
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
		verifyInstanceId();
	}
	
	private void removeInstanceId()
	{
		BoundSetOperations<String, String> setOps = stringRedis.boundSetOps(RPOPLPUSH_DESTN_SET);
		setOps.remove(instanceId);
	}
	@PreDestroy
	private void destroy()
	{
		removeInstanceId();
	}
	
	private void verifyInstanceId() {
		List<RedisClientInfo> clients = redisTemplate.getClientList();
		log.info("No of connected clients => "+clients.size());
		if(log.isDebugEnabled())
		{
			for(RedisClientInfo c : clients)
			{
				log.debug(c.toString());
			}
		}
		
		if(forceApply){
			setInstanceId();
			return;
		}
		compareAndSetInstanceId();
	}
	private void compareAndSetInstanceId()
	{
		BoundSetOperations<String, String> setOps = stringRedis.boundSetOps(RPOPLPUSH_DESTN_SET);
		if(setOps.isMember(instanceId))
		{
			throw new BlazeDuplicateInstanceException("'"+instanceId+"' not allowed");
		}
		long c = setInstanceId(Optional.of(setOps));
		if(c == 0)
			throw new BlazeDuplicateInstanceException("'"+instanceId+"' not allowed");
	}
	private Long setInstanceId()
	{
		return setInstanceId(Optional.ofNullable(null));
	}
	private Long setInstanceId(Optional<BoundSetOperations<String, String>> setOps)
	{
		return setOps.orElse(stringRedis.boundSetOps(RPOPLPUSH_DESTN_SET)).add(instanceId) ;
	}
	/**
	 * Prepare a hash key based on the exchange and route information. Presently it is
	 * simply appended.
	 * @deprecated
	 * @param exchange
	 * @param key
	 * @return
	 */
	private static String prepareHashKey(String exchange, String key)
	{
		return new StringBuilder().append(exchange).append("+").append(key).toString();
	}
	/**
	 * Put the dequeued item to hash.
	 * @param qr
	 * @param key
	 * @deprecated Not needed since using RPOPLPUSH
	 */
	private void prepareCommit(QRecord qr, String key) {/*
		log.debug(">>>>>>>>>> Hash key generated: "+key);
		BoundHashOperations<String, String, QRecord> hashOps = redisTemplate.boundHashOps(key);
		hashOps.put(qr.getKey().getTimeuid().toString(), qr);
	*/}
	/**
	 * Remove the dequeued item from SINK queue. This operation is relatively costly
	 * with complexity of O(n).
	 * @param qr
	 * @param key
	 */
	void endCommit0(QRecord qr, String key) {
		BoundListOperations<String, QRecord> listOps = redisTemplate.boundListOps(prepareInProcKey(key));
		//LREM count < 0: Remove elements equal to value moving from tail to head.
		//since we are pushing from left, the item will be moving towards tail. this operation
		//has a complexity of O(N), so we should try to minimize N
		long c = listOps.remove(-1, qr);
		if (c != 1) {
			c = listOps.remove(1, qr);
			if (c != 1) {
				log.warn("Message was not removed from inproc on endCommit");
			}
		}
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
				
				//the QRecord has been incremented now. So to make the 'remove'
				//operation fire, we will decrement the count to make it equal
				//to the state saved in the inproc queue
				redisTemplate.boundListOps(prepareInProcKey(key)).remove(-1, qr.getRedeliveryCount() > 0 ? qr.copyWithDecrCount() : qr);
				if(enqueueAgain)
				{
					redisTemplate.boundListOps(key).rightPush(qr);
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
	 */
	void reEnqueue0(QRecord qr, String preparedKey) {
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
		listOps.leftPushAll(values);
	}
	//NOTE: Redis keys are data structure specific. So you cannot use the same key for hash and list.
	/**
	 * Prepare a list key based on the exchange and route information. Presently it is
	 * simply appended.
	 * @param exchange
	 * @param key
	 * @return
	 */
	public static String prepareListKey(String exchange, String key)
	{
		return new StringBuilder().append(exchange).append("-").append(key).toString();
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
		return preparedKey + RPOPLPUSH_DESTN_SUFFIX + "." + instanceId;
	}
	@Autowired
	private StringRedisTemplate stringRedis;
	@Autowired
	private BlazeRedisTemplate redisTemplate;
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
	public QRecord dequeue(String xchng, String route, long await, TimeUnit unit) throws TimeoutException
	{
		log.debug(">>>>>>>>>> Start fetchHead <<<<<<<<< ");
		log.debug("route -> " + route + "\tawait: " + await + " unit: " + unit);
		String preparedKey = prepareListKey(xchng, route);
		String inprocKey = prepareInProcKey(preparedKey);
		
		log.debug("dequeue: RPOP "+preparedKey+" LPUSH "+inprocKey);
		QRecord qr = redisTemplate.opsForList().rightPopAndLeftPush(preparedKey, inprocKey, await, unit);
		if (qr != null) {
			return qr;
		}
		throw new TimeoutException();

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
		return  redisTemplate.opsForList().rightPop(preparedKey, await, unit);
		
	}
	/**
	 * Pop the next available item from queue (head), and also put it to a hash via {@link #prepareCommit()}. This is
	 * done to handle message delivery in case of failed attempts.
	 * @deprecated
	 * @param xchng
	 * @param route
	 * @param await
	 * @param unit
	 * @return
	 * @throws TimeoutException
	 */
	@SuppressWarnings("unused")
	private QRecord dequeue_deprecated(String xchng, String route, long await, TimeUnit unit) throws TimeoutException
	{
		log.debug(">>>>>>>>>> Start fetchHead <<<<<<<<< ");
		log.debug("route -> "+route+"\tawait: "+await+" unit: "+unit);
		String preparedKey = prepareListKey(xchng, route);
		BoundListOperations<String, QRecord> listOps = redisTemplate.boundListOps(preparedKey);
		
		//here is a point of failure. the item has been dequeued, and prepareCommit not invoked yet
		//somehow the lpop-and-then-set need to be made atomic
		//also till the record is not delivered to a consumer, there will be a possibility for message loss
		QRecord qr = listOps.leftPop(await, unit);
		if(qr != null)
		{
			//not making this asynchronous to maintain state integrity
			try 
			{
				log.debug("List key generated: "+preparedKey);
				prepareCommit(qr, prepareHashKey(xchng, route));
			} 
			catch (Exception e) {
				log.warn("* Message id "+qr.getKey().getTimeuid()+" not prepared for commit. So redelivery will not work! *");
				log.warn("Root cause: "+e.getMessage());
				log.debug("", e);
			}
			
			return qr;
		}
		throw new TimeoutException();
		
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

		List<Object> removed = clear0(listKey);
		log.debug("Removed items: "+removed);
		log.info("Removed items count: "+removed.size());
		return sizeOf(listKey) == 0;
	}
	private List<Object> clear0(final String listKey)
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

		List<Object> removed = clear0(listKey);
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
