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
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.stereotype.Component;

import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.mq.ops.ConsumerOperations;
@Component
public class ConsumerDataAccessor extends BaseDataAccessor implements ConsumerOperations {

	private static final Logger log = LoggerFactory.getLogger(ConsumerDataAccessor.class);
	@Override
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

	@Override
	public QRecord dequeue(String xchng, String route, long await, TimeUnit unit) {
		log.debug(">>>>>>>>>> Start fetchHead <<<<<<<<< ");
		log.debug("route -> " + route + "\tawait: " + await + " unit: " + unit);
		String preparedKey = prepareListKey(xchng, route);
		String inprocKey = prepareInProcKey(preparedKey);
		
		log.debug("dequeue: RPOP "+preparedKey+" LPUSH "+inprocKey);
		return redisTemplate.opsForList().rightPopAndLeftPush(preparedKey, inprocKey, await, unit);
	}

	@Override
	public QRecord pop(String xchng, String route, long await, TimeUnit unit) {
		String preparedKey = prepareListKey(xchng, route);
		QRecord qr =  redisTemplate.opsForList().rightPop(preparedKey, await, unit);
		if(qr != null)
		{
			statsRecorder.recordDequeu(preparedKey);
		}
		return qr;
	}

	@Override
	public boolean reverseDequeue(String xchng, String route) {
		String preparedKey = prepareListKey(xchng, route);
		String inprocKey = prepareInProcKey(preparedKey);
		log.info("reverseDequeue: RPOP "+inprocKey+" LPUSH "+preparedKey);
		QRecord qr = redisTemplate.opsForList().rightPopAndLeftPush(inprocKey, preparedKey);
		return qr != null;
	}

	@Override
	public boolean clearInproc(String xchangeKey, String routeKey) {
		final String listKey = prepareInProcKey(xchangeKey, routeKey);

		List<Object> removed = invokeClearInTransaction(listKey);
		log.debug("Removed items: "+removed);
		log.info("Removed items count: "+removed.size());
		return sizeOf(listKey) == 0;
	}

}
