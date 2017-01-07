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
package com.reactivetechnologies.mq.ops;

import java.util.Properties;
import java.util.Set;

import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.reactivetechnologies.blaze.struct.QRecord;

public interface BaseOperations {

	/**
	 * Return true if Redis is up and running.
	 * @return
	 */
	boolean isRedisAvailable();

	/**
	 * Get the current server status info - all.
	 * @return
	 */
	Properties getServerInfo();

	//TODO: Fetching queue names. Not being able to PERSIST
	/**
	 * Fetch the name of queues being maintained in Redis. Uses the KEYS
	 * command and {@literal QUEUE_PREFIX_PATTERN}, with an O(n) complexity
	 * @return
	 *
	 */
	Set<String> findQueueNames();

	/**
	 * Clears a given queue using a MULTI RPOP iteration.
	 * @param xchangeKey
	 * @param routeKey
	 * @return true if cleared
	 */
	boolean clear(String xchangeKey, String routeKey);

	/**
	 * Get LLEN for the queue
	 * @param listKey
	 * @return 
	 */
	Long sizeOf(String listKey);
	/**
	 * 
	 * @return
	 */
	StringRedisSerializer getKeySerializer();
	/**
	 * Wrapper on {@linkplain RedisTemplate}
	 * @param prepareKey
	 * @return
	 */
	BoundListOperations<String, QRecord> boundListOps(String prepareKey);
	/**
	 * Wrapper on {@linkplain RedisTemplate}
	 * @param redisCallback
	 */
	void executePipelined(RedisCallback<Integer> redisCallback);

}