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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.SyncFailedException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;

import org.springframework.util.Assert;
/**
 * A local FIFO file implementation. Records can be appended to tail and retrieved from head.
 * Uses a {@linkplain RandomAccessFile} for operations. The {@link #addTail()}, {@link #getHead()} 
 * are synchronized operations which will always perform IO.
 * @author esutdal
 *
 */
class QueuedFile implements Closeable {

	static final String DB_FILE_SUFF = ".qdat";
	private volatile int size = 0;
	public int size()
	{
		return size;
	}
	/**
	 * 
	 * @param dir
	 * @param fileName
	 * @throws IOException
	 */
	public QueuedFile(String dir, String fileName, boolean createIfAbsent) throws IOException
	{
		File f = new File(dir);
		if (!f.exists())
			f.mkdirs();

		try {
			File db = new File(f, fileName + DB_FILE_SUFF);
			if (!db.exists()){
				if(createIfAbsent)
					db.createNewFile();
				else
					throw new FileNotFoundException(fileName);
			}
				

			
			open(db);
			
		} catch (IOException e) {
			throw e;
		}

	}
	
	public QueuedFile(String dir, String fileName) throws IOException
	{
		this(dir, fileName, true);

	}
	public Path getFilePath() {
		return filePath;
	}
	private static class QPointer
	{
		@Override
		public String toString() {
			return "QPointer [head=" + head + ", tail=" + tail + "]";
		}
		private long head = -1, tail = -1;
		private static final int SIZEOF = 16;
		byte[] getBytes()
		{
			ByteBuffer bb = ByteBuffer.allocate(SIZEOF);
			bb.putLong(head);
			bb.putLong(tail);
			bb.flip();
			return bb.array();
		}
		void setBytes(byte[] b)
		{
			Assert.isTrue(b != null && b.length == SIZEOF, "Invalid bytes for QPointer");
			ByteBuffer bb = ByteBuffer.wrap(b);
			head = bb.getLong();
			tail = bb.getLong();
		}
	}
	private RandomAccessFile dataFile;
	private volatile QPointer pointer;
	private FileLock fileLock;
	private void createQueuePointer() throws IOException
	{
		//new file. create queue pointer
		pointer = new QPointer();
		updatePointer();
		
		pointer.head = dataFile.getFilePointer();
		pointer.tail = pointer.head;
		
		updatePointer();
	}
	private void initQueuePointer() throws IOException
	{
		//existing file. read queue pointer
		dataFile.seek(0);
		byte[] b = new byte[QPointer.SIZEOF];
		dataFile.readFully(b);
		pointer = new QPointer();
		pointer.setBytes(b);
		
		readSize();
	}
	private void readSize() throws IOException
	{
		dataFile.seek(pointer.head);
		while (dataFile.getFilePointer() < pointer.tail) {
			int len = dataFile.readInt();
			dataFile.skipBytes(len);
			size++;
		}
		
	}
	private void updatePointer() throws IOException
	{
		dataFile.seek(0);
		dataFile.write(pointer.getBytes());
	}
	private byte[] read() throws IOException
	{
		int len = dataFile.readInt();
		byte[] b = new byte[len];
		dataFile.readFully(b);
		return b;
	}
	private void updateHead() throws IOException
	{
		pointer.head = dataFile.getFilePointer();
		updatePointer();
	}
	private void updateTail() throws IOException
	{
		pointer.tail = dataFile.getFilePointer();
		updatePointer();
	}
	synchronized byte[] getHead() throws IOException
	{
		dataFile.seek(pointer.head);
		byte[] b = read();
		updateHead();
		sync();
		size--;
		return b;
	}
	
	public boolean isEmpty()
	{
		return pointer.head == pointer.tail;
	}
	private void write(byte[] b) throws IOException
	{
		dataFile.writeInt(b.length);
		dataFile.write(b);
	}
	synchronized void addTail(byte[] b) throws IOException
	{
		dataFile.seek(pointer.tail);
		write(b);
		updateTail();
		sync();
		size++;
	}
	private Path filePath;
	private void open(File dbFile) throws IOException {

		dataFile  = new RandomAccessFile(dbFile, "rwd");
		if(!acquire())
		{
			throw new IOException(new IllegalStateException("File already in use"));
		}
		if(dataFile.length() == 0)
		{
			createQueuePointer();
		}
		else
		{
			initQueuePointer();
		}
		closed = false;
		filePath = dbFile.toPath();
	}
	/**
	 * Reopen operation for a file which has been {@link #delete()}ed.
	 * @throws IOException
	 */
	public void open() throws IOException
	{
		Assert.notNull(filePath, "No file path has been specified yet");
		Assert.isTrue(!closed, "Already open");
		
		open(filePath.toFile());
	}
	private boolean acquire() throws IOException {
		fileLock = dataFile.getChannel().tryLock();
		return fileLock != null;
	}
	@Override
	public void close() throws IOException {
		if (fileLock != null) {
			fileLock.release();
		}
		if (dataFile != null) {
			dataFile.close();
		}
		fileLock = null;
		dataFile = null;
		closed = true;
	}

	private volatile boolean closed = false;
	private void sync() throws SyncFailedException, IOException
	{
		dataFile.getFD().sync();
	}
	public boolean delete()
	{
		try {
			if (!closed) {
				close();
			}
			Files.delete(filePath);
		} catch (IOException e) {
			// ignored
			e.printStackTrace();
			return false;
		}
		return true;
		
	}
	public static void main(String[] args) throws IOException {/*
		byte[] m1 = "MSG1".getBytes();
		byte[] m2 = "MSG2".getBytes();
		byte[] m3 = "MSG3".getBytes();
		
		QueuedFile qf = null;
		try {
			qf = new QueuedFile(".", "test_file");
			System.out.println(qf.isEmpty());
			System.out.println(qf.size());
			
			//gets
			System.out.println(new String(qf.getHead()));
			System.out.println(new String(qf.getHead()));
			System.out.println(new String(qf.getHead()));
			
			//adds
			qf.addTail(m1);
			qf.addTail(m2);
			qf.addTail(m3);
			
		} finally {
			qf.close();
			if(qf.isEmpty())
				qf.delete();
		}
		
	*/}
}
