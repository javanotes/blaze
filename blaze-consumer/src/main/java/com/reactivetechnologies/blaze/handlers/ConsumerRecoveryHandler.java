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
package com.reactivetechnologies.blaze.handlers;

public interface ConsumerRecoveryHandler {

	/**
	 * Handle all pending data which are still lying in INPROC queue. This will be invoked
	 * once before the consumer starts.
	 * @param exchange
	 * @param route
	 */
	void handle(String exchange, String route);
}
