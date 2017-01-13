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
import java.util.Arrays;

/**
 * Memory mapped file corresponding to head region.
 * @author esutdal
 *
 */
class MMapHeadSeq implements Closeable
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
	public MMapHeadSeq(FileBackedQueue fileBackedQueue) throws IOException {
		this.fileBackedQueue = fileBackedQueue;
		mappedBuff = this.fileBackedQueue.getDataFile().getChannel().map(MapMode.READ_ONLY, this.fileBackedQueue.getPointer().getHead(), FileBackedQueue.MAP_CHUNK_SIZE);
		setPosition(this.fileBackedQueue.getPointer().getHead());
	}
	
	private byte[] remap() throws IOException
	{
		byte[] b = new byte[mappedBuff.remaining()];
		mappedBuff.get(b);
		setPosition(getPosition() + b.length);
		DirectMem.unmap(mappedBuff);
		mappedBuff = this.fileBackedQueue.getDataFile().getChannel().map(MapMode.READ_ONLY, getPosition(), FileBackedQueue.MAP_CHUNK_SIZE);
		return b;
	}
	private byte[] getAvailable(int reqd) throws IOException
	{
		if(mappedBuff.remaining() < reqd)
		{
			System.out.println("FileBackedQueue.HeadMemoryMap.makeAvailable() remain " + mappedBuff.remaining()
					+ " cap " + mappedBuff.capacity() + " pos " + mappedBuff.position());
			
			byte[] leftover = remap();
			if(leftover.length == reqd){
				return leftover;
			}
			else
			{
				int reqdmore = reqd-leftover.length;
				byte[] rest = getAvailable(reqdmore);
				leftover = Arrays.copyOf(leftover, reqd);
				System.arraycopy(rest, 0, leftover, reqdmore, rest.length);
				
				return leftover;
			}
			/*synchronized (this.fileBackedQueue.mmapMutex) {
				if(mappedBuff.remaining() < reqd)
				{
					mappedBuff.get(new byte[mappedBuff.remaining()]);
					try {
						this.fileBackedQueue.mmapMutex.wait();//block indefinitely
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
					}
				}
			}*/
		}
		byte[] b = new byte[reqd];
		mappedBuff.get(b);
		return b;
	}
	public synchronized byte[] read() throws IOException
	{
		try 
		{
			getAvailable(4);
			int len = mappedBuff.getInt();
			setPosition(getPosition() + 4);
			byte[] b = new byte[len];
			getAvailable(len);
			mappedBuff.get(b);
			setPosition(getPosition() + len);
			return b;
		} catch (Exception e) {
			throw new IOException("On mapped byte buffer read", e);
		}
	}
	
	@Override
	public void close() throws IOException {
		if(mappedBuff != null){
			DirectMem.unmap(mappedBuff);
			
			synchronized (this.fileBackedQueue) {
				this.fileBackedQueue.getPointer().setHead(getPosition());
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