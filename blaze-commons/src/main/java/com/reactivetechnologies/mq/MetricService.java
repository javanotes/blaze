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
package com.reactivetechnologies.mq;

public interface MetricService {

	long getDequeueCount(String route);
	/**
	 * 
	 * @param exchange
	 * @param route
	 * @return
	 */
	long getDequeueCount(String exchange, String route);
	/**
	 * 
	 * @param exchange
	 * @param route
	 * @return
	 */
	long getEnqueueCount(String exchange, String route);
	long getEnqueueCount(String route);
	/**
	 * 
	 * @param exchange
	 * @param route
	 */
	void resetCounts(String exchange, String route);
	void resetCounts(String route);
}
