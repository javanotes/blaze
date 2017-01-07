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

import com.reactivetechnologies.blaze.struct.QRecord;

public interface ProducerOperations extends BaseOperations{

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
	void enqueue(String preparedKey, QRecord... values);

	/**
	 * Perform a plain LPUSH operation. This is generally a part of the more comprehensive {@link #enqueue()} operation.
	 * @param preparedKey
	 * @param items
	 */
	void lpushAll(String preparedKey, QRecord[] items);

	String prepareListKey(String exchange, String key);

}