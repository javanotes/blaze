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
package com.reactivetechnologies.mq.container;

import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.mq.Data;
import com.reactivetechnologies.mq.consume.QueueListener;

public interface QueueContainer extends Runnable{

	/**
	 * Register a queue listener.
	 * @param <T>
	 * @param listener
	 */
	<T extends Data> void register(QueueListener<T> listener);
	
	/**
	 * 
	 * @param qr
	 * @param success
	 */
	void commit(QRecord qr, boolean success);
	
	/**
	 * 
	 * @param qr
	 */
	void rollback(QRecord qr);

	/**
	 * 
	 * @return
	 */
	long getPollInterval();
	/**
	 * 
	 * @param pollInterval
	 */
	void setPollInterval(long pollInterval);
	/**
	 * 
	 */
	void destroy();
	
}