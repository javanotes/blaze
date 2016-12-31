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

import java.nio.charset.StandardCharsets;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.blaze.struct.QRecordSerializer;

/**
 * 
 * The {@linkplain RedisTemplate} to be used by RMQ operations.
 *
 */
public class BlazeRedisTemplate extends RedisTemplate<String, QRecord>
{
	private StringRedisSerializer keySerializer = new StringRedisSerializer(StandardCharsets.UTF_8);
	private QRecordSerializer valueSerializer = new QRecordSerializer();
	
	public void setSerializers()
	{
		setKeySerializer(keySerializer);
		setHashKeySerializer(keySerializer);
		setValueSerializer(valueSerializer);
		setHashValueSerializer(valueSerializer);
	}
	
}