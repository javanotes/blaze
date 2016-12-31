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

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.springframework.util.Assert;

import com.reactivetechnologies.blaze.reqres.RequestReplyReceiver;
import com.reactivetechnologies.mq.Data;
import com.reactivetechnologies.mq.QueueService;
import com.reactivetechnologies.mq.RequestReplyAble;
/**
 * Synchronous operations involving a request and reply queue.
 * @author esutdal
 *
 */
public class RequestReplyServiceImpl implements RequestReplyAble {
	/**
	 * New instance, passing arguments.
	 * @param callback
	 * @param qService
	 */
	public RequestReplyServiceImpl(RequestReplyReceiver callback, QueueService qService) {
		super();
		this.callback = callback;
		this.qService = qService;
	}

	private RequestReplyReceiver callback;
	private QueueService qService;
	
	@Override
	public <T extends Data> Data sendAndReceive(T request, long await, TimeUnit unit) {
		if(request.getCorrelationID() == null)
			request.setCorrelationID(UUID.randomUUID().toString());
		
		qService.add(Arrays.asList(request));
		Data d = callback.awaitAndGet(request.getCorrelationID(), await, unit);
		return d;
	}

	@Override
	public <T extends Data> String send(T request) {
		if(request.getCorrelationID() == null)
			request.setCorrelationID(UUID.randomUUID().toString());
		qService.add(Arrays.asList(request));
		
		return request.getCorrelationID();
	}

	@Override
	public Data receive(String correlationId, long await, TimeUnit unit) {
		Assert.notNull(correlationId);
		return callback.awaitAndGet(correlationId, await, unit);
	}

}
