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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.mq.QueueService;
import com.reactivetechnologies.mq.ops.BaseOperations;

public abstract class BaseDataAccessor implements BaseOperations {

	static final String RPOPLPUSH_DESTN_SUFFIX = "$INPROC";
	public static final String RPOPLPUSH_DESTN_SET = RPOPLPUSH_DESTN_SUFFIX + "-SET";
	public static final String QUEUE_PREFIX = "queues/";
	public static final String QUEUE_PREFIX_PATTERN = QUEUE_PREFIX+"*";
	public static final String STATS_SUFFIX = "$STAT";
	static final String LIST_KEY_JOIN_SEPARATOR = "-";
	static final String INPROC_KEY_JOIN_SEPARATOR = ".";
	private static final Logger log = LoggerFactory.getLogger(BaseDataAccessor.class);
	
	public String prepareInProcKey(String exchange, String key)
	{
		String preparedKey = prepareListKey(exchange, key);
		return prepareInProcKey(preparedKey);
	}
	/**
	 * Size of queue.
	 * @param xchangeKey
	 * @param routeKey
	 * @return
	 */
	public long size(String xchangeKey, String routeKey) {
		return sizeOf(prepareListKey(xchangeKey, routeKey));
	}
	public long size(String queue) {
		return size(QueueService.DEFAULT_XCHANGE, queue);
	}
	/**
	 * 
	 * @param preparedKey
	 * @return
	 */
	protected String prepareInProcKey(String preparedKey)
	{
		return preparedKey + RPOPLPUSH_DESTN_SUFFIX + INPROC_KEY_JOIN_SEPARATOR + instanceService.getInstanceId();
	}
	
	@Autowired StringRedisTemplate stringRedis;
	@Autowired BlazeRedisTemplate redisTemplate;
	@Autowired
	protected RedisStatsRecorder statsRecorder;
	@Autowired
	protected InstanceInitializationService instanceService;
	@Override
	public boolean isRedisAvailable() {
		try {
			RedisConnection conn = stringRedis.getConnectionFactory().getConnection();
			conn.close();//return to pool
			return true;
		} catch (RedisConnectionFailureException e) {
			log.error("* Redis is unavailable * Root cause => "+ e.getMostSpecificCause().getMessage());
			log.error("Full stacktrace provided in debug log");
			log.debug("", e);
		}
		return false;
	}
	/**
	 * To be overridden as needed.
	 */
	protected void doInit()
	{
		instanceService.verifyInstanceId();
	}
	/**
	 * To be overridden as needed.
	 */
	protected void doDestroy()
	{
		instanceService.removeInstanceId();
		instanceService.persistQueueNames(findQueueNames());
	}
	
	@Override
	public Properties getServerInfo() {
		return redisTemplate.execute(new RedisCallback<Properties>() {

			@Override
			public Properties doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.info("all");
			}
		});
	}

	@Override
	public Set<String> findQueueNames() {
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
		//persistQueueName(name);
		return name;
	}
	protected List<Object> invokeClearInPipeline(final String listKey)
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
	protected List<Object> invokeClearInTransaction(final String listKey)
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
	@Override
	public boolean clear(String xchangeKey, String routeKey) {
		final String listKey = prepareListKey(xchangeKey, routeKey);

		//it is better to run clear in pipeline for faster execution
		//anyway we return a boolean to confirm
		//so clients higher up can do a compare and set type operation.
		List<Object> removed = invokeClearInPipeline(listKey);
		
		log.debug("Removed items: "+removed);
		log.info("Removed items count: "+removed.size());
		return sizeOf(listKey) == 0;
	}

	@Override
	public Long sizeOf(String listKey) {
		return boundListOps(listKey).size();
	}

	@Override
	public StringRedisSerializer getKeySerializer() {
		return (StringRedisSerializer) redisTemplate.getKeySerializer();
	}

	@Override
	public BoundListOperations<String, QRecord> boundListOps(String prepareKey) {
		return redisTemplate.boundListOps(prepareKey);
	}

	@Override
	public void executePipelined(RedisCallback<Integer> redisCallback) {
		redisTemplate.executePipelined(redisCallback);
	}

}
