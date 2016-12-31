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
package com.reactivetechnologies.blaze.reqres;

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactivetechnologies.mq.Data;

class DiskCacheLoader implements Closeable{

	private static final Logger log = LoggerFactory.getLogger(DiskCacheLoader.class);
	private final BasicDurableMap bdMap;
	/**
	 * 
	 * @param dir
	 * @param fileName
	 */
	public DiskCacheLoader(String dir, String fileName) {
		super();
		this.bdMap = new BasicDurableMap(dir, fileName);
	}
	public void loadFromDisk() {
		// TODO Auto-generated method stub
		log.info("TODO: loading from disk");
	}
	public void delete()
	{
		
	}
	public void dumpToDisk(Data m) {
		// TODO Auto-generated method stub
		log.warn("TODO dump to disk: "+m);
	}
	@Override
	public void close() throws IOException {
		bdMap.close();
	}

}
