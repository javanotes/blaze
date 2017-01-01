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

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.BoundHashOperations;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
@Component
public class RedisStatsRecorder {

	static final String HASH_SUFFIX = "$HASH";
	static final String STATS_ENQ = "STATS_ENQ";
	static final String STATS_DEQ = "STATS_DEQ";
	static final String STATS_LEN = "STATS_LEN";
	@Autowired
	private StringRedisTemplate stringRedis;
	
	private static String hashKey(String key)
	{
		return key + HASH_SUFFIX;
	}
	
	public void recordEnqueu(String key)
	{
		recordEnqueu(key, 1);
	}
	public void recordEnqueu(String key, long c)
	{
		stringRedis.boundHashOps(hashKey(key)).increment(STATS_ENQ, c);
	}
	private long getStats(String key, String op)
	{
		BoundHashOperations<String, Object, Object> ops = stringRedis.boundHashOps(hashKey(key));
		try {
			Object count = ops.get(op);
			return Long.valueOf(count.toString());
		} catch (NullPointerException e) {
			//not checking if field exists
			//don't want to catch NumberFormatException however, since that will be due to programming error
			return 0;
		}
	}
	public long getEnqueuStats(String key)
	{
		return getStats(key, STATS_ENQ);
	}
	public long getDequeuStats(String key)
	{
		return getStats(key, STATS_DEQ);
	}
	public void recordDequeu(String key)
	{
		recordDequeu(key, 1);
	}
	public void recordDequeu(String key, int decr)
	{
		stringRedis.boundHashOps(hashKey(key)).increment(STATS_DEQ, Math.negateExact(decr));
	}
	public void reset(String key)
	{
		BoundHashOperations<String, Object, Object> ops = stringRedis.boundHashOps(hashKey(key));
		stringRedis.execute(new SessionCallback<List<Object>>() {

			@Override
			public <K, V> List<Object> execute(RedisOperations<K, V> operations) throws DataAccessException {
				operations.multi();
				ops.delete(STATS_ENQ, STATS_DEQ);
				ops.increment(STATS_DEQ, 0);
				ops.increment(STATS_ENQ, 0);
				return operations.exec();
			}
		});
		
		
	}

}
