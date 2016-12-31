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
package com.reactivetechnologies.blaze.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.reactivetechnologies.blaze.handlers.ConsumerRecoveryHandler;
import com.reactivetechnologies.blaze.ops.RedisDataAccessor;
//TODO
public class DefaultConsumerRecoveryHandler implements ConsumerRecoveryHandler {
	/**
	 * 
	 * @param redisOps
	 */
	public DefaultConsumerRecoveryHandler() {
		super();
	}
	
	@Autowired
	private RedisDataAccessor redisOps;
	private static final Logger log = LoggerFactory.getLogger(DefaultConsumerRecoveryHandler.class);
	
	private long rpopInprocNlpushSource(String route, String exchange)
	{
		long count = 0;
		boolean did = false;
		do {
			count++;
			did = redisOps.queuede(exchange, route);
		} while (did);
		return count;
	}
	private void recoverIfPresent(String exchange, String route)
	{
		String listKey = redisOps.prepareInProcKey(exchange, route);
		long size = redisOps.sizeOf(listKey);
		
		if(size > 0)
		{
			log.info("Will recover "+size+" items for re-enqueue");
			long count = rpopInprocNlpushSource(route, exchange);
			
			if(count != size)
			{
				log.warn("Expected to process "+size+" items, but found "+count);
			}
		}
	}
	@Override
	public void handle(String exchange, String route) {
		
		log.info("Checking in-processing messages for exchange '"+exchange+"', route '"+route+"'");
		recoverIfPresent(exchange, route);
	}

}
