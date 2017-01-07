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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.reactivetechnologies.blaze.struct.QRecord;

public interface ConsumerOperations extends BaseOperations{

	/**
	 * Mark the end of a commit phase. The action would be to remove message from inproc queue.
	 * Also, if enqueueAgain is true, message will be enqueued at head of source queue. The operation
	 * happens atomically from within a MULTI block.
	 * 
	 * @param qr
	 * @param key
	 * @param enqueueAgain
	 */
	void endCommit(QRecord qr, String key, boolean enqueueAgain);

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
	QRecord dequeue(String xchng, String route, long await, TimeUnit unit);

	/**
	 * RPOP operation. This method should be used in message polling scenario. For a reliable messaging,
	 * like in consumer deliveries, {@link #dequeue()} should be considered.
	 * @param xchng
	 * @param route
	 * @return dequeued item or null
	 */
	QRecord pop(String xchng, String route, long await, TimeUnit unit);

	/**
	 * Enqueue items from the recovery (inproc) queue. These items will be appended at tail
	 * of the source queue. So it is possible to get a backdated item popped now. This method
	 * is thus opposite to  the {@link #dequeue()} method in action. 
	 * @param xchng
	 * @param route 
	 * @return 
	 */
	boolean reverseDequeue(String xchng, String route);
	/**
	 * Clear the INPROCESS queue involved in reliable message delivery.
	 * @param xchangeKey
	 * @param routeKey
	 * @return
	 */
	boolean clearInproc(String xchangeKey, String routeKey);

}