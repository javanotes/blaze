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
package com.reactivetechnologies.mq.disk;

import java.io.Closeable;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import com.reactivetechnologies.mq.exceptions.BlazeInternalException;

/**
 * Memory mapped file corresponding to tail region.
 * @author esutdal
 *
 */
class MMapTailSeq implements Closeable
{
	/**
	 * 
	 */
	private final FileBackedQueue fileBackedQueue;
	private MappedByteBuffer mappedBuff;
	private volatile long position = 0;
	
	/**
	 * 
	 * @param fileBackedQueue TODO
	 * @param isHeadMap
	 * @throws IOException
	 */
	public MMapTailSeq(FileBackedQueue fileBackedQueue) throws IOException {
		this.fileBackedQueue = fileBackedQueue;
		mappedBuff = this.fileBackedQueue.getDataFile().getChannel().map(MapMode.READ_WRITE, this.fileBackedQueue.getPointer().getTail(), FileBackedQueue.MAP_CHUNK_SIZE);
		setPosition(this.fileBackedQueue.getPointer().getTail());
	}
	private void makeAvailable(int reqd) throws IOException
	{
		if (mappedBuff.remaining() < reqd) {
			mappedBuff.force();
			DirectMem.unmap(mappedBuff);
			mappedBuff = null;
			mappedBuff = this.fileBackedQueue.getDataFile().getChannel().map(MapMode.READ_WRITE, getPosition(), FileBackedQueue.MAP_CHUNK_SIZE);
			
		}
	}
	public synchronized void write(byte[] b) throws IOException
	{
		try 
		{
			makeAvailable(4);
			mappedBuff.putInt(b.length);
			setPosition(getPosition() + 4);
			makeAvailable(b.length);
			mappedBuff.put(b);
			setPosition(getPosition() + b.length);
			
			synchronized (this.fileBackedQueue.mmapMutex) {
				this.fileBackedQueue.mmapMutex.notify();
			}
			
		} catch (Exception e) {
			throw new BlazeInternalException("On mapped byte buffer write", e);
		}
		
	}

	@Override
	public void close() throws IOException  {
		if(mappedBuff != null){
			DirectMem.unmap(mappedBuff);
			
			synchronized (this.fileBackedQueue) {
				this.fileBackedQueue.getPointer().setTail(getPosition());
				this.fileBackedQueue.writePointers();
			}
		}
		
		
	}
	public long getPosition() {
		return position;
	}
	public void setPosition(long position) {
		this.position = position;
	}
}