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
package com.reactivetechnologies.mq;

import java.util.concurrent.TimeUnit;
/**
 * Synchronous execution involving a request and reply queue, correlated by a correlationId.
 * @author esutdal
 *
 */
public interface RequestReplyAble {

	/**
	 * Request-reply semantic for synchronous execution.
	 * @param request
	 * @param await
	 * @param unit
	 * @return
	 */
	<T extends Data> Data sendAndReceive(T request, long await, TimeUnit unit);
	/**
	 * Request for asynchronous execution.
	 * @param request
	 * @return correlationId
	 */
	<T extends Data> String send(T request);
	/**
	 * Response for asynchronous operation.
	 * @param correlationId
	 * @param await
	 * @param unit
	 * @return
	 */
	Data receive(String correlationId, long await, TimeUnit unit);
}
