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

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.BoundSetOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.types.RedisClientInfo;
import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.stereotype.Component;

import com.reactivetechnologies.mq.exceptions.BlazeDuplicateInstanceException;
@Component
class InstanceInitializationService
{
	private static final Logger log = LoggerFactory.getLogger(InstanceInitializationService.class);
	
	@Value("${blaze.instance.id}") private String instanceId;
	public String getInstanceId() {
		return instanceId;
	}

	public void persistListKeys(Set<String> qNames)
	{
		redisTemplate.executePipelined(new RedisCallback<Void>() {

			@SuppressWarnings("unchecked")
			@Override
			public Void doInRedis(RedisConnection connection) throws DataAccessException {
				RedisSerializer<String> ser = (RedisSerializer<String>) redisTemplate.getKeySerializer();
				for(String qName : qNames)
				{
					connection.persist(ser.serialize(qName));
				}
				return null;
			}
		});
	}
	public void persistQueueNames(Set<String> qNames) {
		persistListKeys(qNames);
		
	}

	
	public void removeInstanceId()
	{
		BoundSetOperations<String, String> setOps = stringRedis.boundSetOps(BaseDataAccessor.RPOPLPUSH_DESTN_SET);
		setOps.remove(instanceId);
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}
	@Value("${blaze.instance.id.force:false}") boolean forceApply;
	@Autowired StringRedisTemplate stringRedis;
	@Autowired BlazeRedisTemplate redisTemplate;
	
	private void setClientId() {
		redisTemplate.execute(new RedisCallback<Void>() {

			@Override
			public Void doInRedis(RedisConnection connection) throws DataAccessException {
				connection.setClientName(getInstanceId().getBytes(StandardCharsets.UTF_8));
				return null;
			}
		});
	}

	
	private Long setInstanceId()
	{
		return setInstanceId(Optional.ofNullable(null));
	}
	private Long setInstanceId(Optional<BoundSetOperations<String, String>> setOps)
	{
		return setOps.orElse(stringRedis.boundSetOps(BaseDataAccessor.RPOPLPUSH_DESTN_SET)).add(instanceId) ;
	}
	
	private void compareAndSetInstanceId()
	{
		BoundSetOperations<String, String> setOps = stringRedis.boundSetOps(BaseDataAccessor.RPOPLPUSH_DESTN_SET);
		if(setOps.isMember(instanceId))
		{
			throw new BlazeDuplicateInstanceException("'"+instanceId+"' not allowed");
		}
		long c = setInstanceId(Optional.of(setOps));
		if(c == 0)
			throw new BlazeDuplicateInstanceException("'"+instanceId+"' not allowed");
	}
	private void compareAndSet()
	{
		if(forceApply){
			setInstanceId();
			return;
		}
		compareAndSetInstanceId();
	}
	public void verifyInstanceId() {

		compareAndSet();
		setClientId();
		
		List<RedisClientInfo> clients = redisTemplate.getClientList();
		log.info("No of connected clients -> "+clients.size());
		if(log.isDebugEnabled())
		{
			for(RedisClientInfo c : clients)
			{
				log.debug("RedisClientInfo: "+c.toString());
			}
			
			
		}
	}
}