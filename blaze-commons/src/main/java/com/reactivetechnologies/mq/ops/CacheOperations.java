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

public interface CacheOperations {

	/**
	 * Put a key, value to Redis map. This is basically a cache
	 * wrapper operation.
	 * @param key
	 * @param value
	 */
	<V, K> void put(String cache, K key, V value);

	/**
	 * Get the value corresponding to a {@link #put()} operation. This is basically a cache
	 * wrapper operation.
	 * @param cache
	 * @param key
	 * @return The value or null if not present.
	 */
	<V, K> V get(String cache, K key);

}