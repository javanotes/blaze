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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Collection;
import java.util.NoSuchElementException;

import com.reactivetechnologies.mq.DataSerializable;
import com.reactivetechnologies.mq.common.BlazeInternalException;
/**
 * A wrapper on {@linkplain QueuedFile} to be used as a generic queue.
 * @author esutdal
 *
 * @param <E>
 */
public class LocalDurableQueue<E extends DataSerializable> implements Closeable{
	/**
	 * Get the absolute path of the underlying file.
	 * @return
	 */
	public String getPath()
	{
		return file.getFilePath().toAbsolutePath().toString();
	}
	/**
	 * 
	 * @param type
	 * @param fileName
	 * @param dir
	 */
	public LocalDurableQueue(Class<E> type, String fileName, String dir) {
		this(type, fileName, dir, true);
	}
	private String directory;
	public String[] listFilesInDir()
	{
		File fir = new File(directory);
		if(fir.isDirectory())
		{
			return fir.list(new FilenameFilter() {
				
				@Override
				public boolean accept(File dir, String name) {
					File f = new File(dir, name);
					return f.isFile() && name.endsWith(QueuedFile.DB_FILE_SUFF);
				}
			});
		}
		return null;
	}
	/**
	 * 
	 * @param type
	 * @param fileName
	 * @param dir
	 * @param createIfAbsent
	 * @throws IOException 
	 */
	public LocalDurableQueue(Class<E> type, String fileName, String dir, boolean createIfAbsent) {
		super();
		this.type = type;
		directory = dir;
		try {
			file = new QueuedFile(dir, fileName, createIfAbsent);
		} catch (IOException e) {
			throw new BlazeInternalException("Unable to open queue file", e);
		}
	}

	private QueuedFile file;
	private final Class<E> type;
	/**
	 * Add all items one by one. This method simply invokes {@link #add()} iteratively.
	 * @param items
	 * @return
	 */
	public boolean addAll(Collection<? extends E> items) {
		for(E each : items)
		{
			add(each);
		}
		return true;
	}
	/**
	 * Delete the associated file and release resources.
	 */
	public void destroy() {
		file.delete();
	}

	/**
	 * If queue is empty
	 * @return
	 */
	public boolean isEmpty() {
		return file.isEmpty();
	}
	/**
	 * Current size of the queue. Note, the file size is always incremental though.
	 * So it is advisable to {@link #destroy()} an empty instance to free up file system.
	 * @return
	 */
	public int size() {
		return file.size();
	}
	/**
	 * Add next item to queue at tail.
	 * @param item
	 * @return
	 */
	public boolean add(E item) {
		try 
		{
			file.addTail(objectToBytes(item));
			return true;
		} 
		catch (IOException e) {
			throw new BlazeInternalException("While enqueuing to QueuedFile", e);
		}
	}

	private byte[] objectToBytes(E e) throws IOException
	{
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		e.writeData(new DataOutputStream(out));
		byte[] b = out.toByteArray();
		//System.out.println("LocalDurableQueue.objectToBytes() length: "+b.length);
		return b;
	}
	private E bytesToObject(byte[] b) throws InstantiationException, IllegalAccessException, ClassNotFoundException, IOException
	{
		E e = type.newInstance();
		//System.out.println("LocalDurableQueue.bytesToObject() length: "+b.length);
		e.readData(new DataInputStream(new ByteArrayInputStream(b)));
		return e;
	}
	/**
	 * Dequeue and return the head of the queue if available, else return null.
	 * @return
	 */
	public E poll() {
		if(file.isEmpty())
			return null;
		
		return take();
	}
	
	private E take()
	{
		try 
		{
			byte[] b = file.getHead();
			return bytesToObject(b);
		} 
		catch (Exception e) {
			throw new BlazeInternalException("While dequeuing from QueuedFile", e);
		}
	}
	/**
	 * Remove and return the head of the queue. This method is similar to {@link #poll()}
	 * only that it will throw an exception if item remains to be dequeued.
	 * @return
	 */
	public E remove() {
		if(isEmpty())
			throw new NoSuchElementException();
		return take();
	}
	/**
	 * Close the underlying file. This does not delete the file. Use {@link #destroy()} 
	 * to do that.
	 */
	@Override
	public void close() throws IOException {
		file.close();
	}

}
