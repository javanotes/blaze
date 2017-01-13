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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
/**
 * A local FIFO file implementation. Records can be appended to tail and retrieved from head.
 * Uses a {@linkplain RandomAccessFile} for operations. The {@link #addTail()}, {@link #getHead()} 
 * are synchronized operations which will always perform IO.
 * @author esutdal
 *
 */
class FileBackedQueue implements Closeable {

	static final String DB_FILE_SUFF = ".qdat";
	static final long MAP_CHUNK_SIZE = 8192;
	private final AtomicInteger size = new AtomicInteger();
	public int size()
	{
		return size.get();
	}
	/**
	 * Constructor for new instance.
	 * @param dir
	 * @param fileName
	 * @param createIfAbsent
	 * @param usingMemoryMappedIO
	 * @throws IOException
	 */
	public FileBackedQueue(String dir, String fileName, boolean createIfAbsent, boolean usingMemoryMappedIO) throws IOException
	{
		this.useMemMappedIO = usingMemoryMappedIO;
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
			startSyncThread();
			
		} catch (IOException e) {
			throw e;
		}

	}
	/**
	 * Constructor.
	 * @param dir
	 * @param fileName
	 * @param createIfAbsent
	 * @throws IOException
	 */
	public FileBackedQueue(String dir, String fileName, boolean createIfAbsent) throws IOException
	{
		this(dir, fileName, createIfAbsent, false);
	}
	private ScheduledExecutorService syncThread;
	private void startSyncThread() 
	{
		syncThread = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
			
			@Override
			public Thread newThread(Runnable r) {
				Thread t = new Thread(r, "QueuedFile.SyncThread-"+StringUtils.getFilename(getFilePath().toString()));
				t.setDaemon(true);
				return t;
			}
		});
		// If any execution of the task encounters an exception, subsequent executions are suppressed.
		//however, we can probably ignore it for our cause, since we are catching the exception.
		syncThread.scheduleWithFixedDelay(new Runnable() {
			
			@Override
			public void run() {
				try {
					sync(false);
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}, 1, 1, TimeUnit.SECONDS);
	}
	/**
	 * 
	 * @param dir
	 * @param fileName
	 * @throws IOException
	 */
	public FileBackedQueue(String dir, String fileName) throws IOException
	{
		this(dir, fileName, true);

	}
	
	public Path getFilePath() {
		return filePath;
	}
	static class QPointer
	{
		@Override
		public String toString() {
			return "QPointer [head=" + head + ", tail=" + tail + "]";
		}
		private long head = -1;
		public long getHead() {
			return head;
		}
		public void setHead(long head) {
			this.head = head;
		}
		private long tail = -1;
		public long getTail() {
			return tail;
		}
		public void setTail(long tail) {
			this.tail = tail;
		}
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
	public RandomAccessFile getDataFile() {
		return dataFile;
	}
	private volatile QPointer pointer;
	public QPointer getPointer() {
		return pointer;
	}
	private FileLock fileLock;
	private void createQueuePointer() throws IOException
	{
		//new file. create queue pointer
		pointer = new QPointer();
		writePointers();
		
		pointer.head = dataFile.getFilePointer();
		pointer.tail = pointer.head;
		
		writePointers();
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
			size.incrementAndGet();
		}
		
	}
	//write operations
	private final AtomicBoolean isDirty = new AtomicBoolean();
	private void writeData(byte[] b) throws IOException
	{
		dataFile.writeInt(b.length);
		dataFile.write(b);
		isDirty.compareAndSet(false, true);
	}
	void writePointers() throws IOException
	{
		dataFile.seek(0);
		dataFile.write(pointer.getBytes());
		isDirty.compareAndSet(false, true);
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
		writePointers();
	}
	private void updateTail() throws IOException
	{
		pointer.tail = dataFile.getFilePointer();
		writePointers();
	}
	/**
	 * Get data from the head of the queue.
	 * @return
	 * @throws IOException
	 */
	public final byte[] getHead() throws IOException
	{
		byte[] b = useMemMappedIO ? getFromMMappedFile() : getFromDataFile();
		size.decrementAndGet();
		return b;
		
	}
	private synchronized byte[] getFromDataFile() throws IOException
	{
		dataFile.seek(pointer.head);
		byte[] b = read();
		updateHead();
		return b;
	}
	private byte[] getFromMMappedFile() throws IOException
	{
		return headMMap.read();
	}
	public boolean isEmpty()
	{
		return useMemMappedIO ? isEmpty0() : pointer.head == pointer.tail;
	}
	private boolean isEmpty0()
	{
		return tailMMap.getPosition() == headMMap.getPosition();
	}
	private final boolean useMemMappedIO;
	/**
	 * Add data to the tail of the queue.
	 * @param b
	 * @throws IOException
	 */
	public final void addTail(byte[] b) throws IOException
	{
		if (useMemMappedIO) {
			addToMMappedFile(b);
		}
		else
			addToDataFile(b);
		
		size.incrementAndGet();
		
	}
	private synchronized void addToDataFile(byte[] b) throws IOException
	{
		dataFile.seek(pointer.tail);
		writeData(b);
		updateTail();
	}
	private void addToMMappedFile(byte[] b) throws IOException
	{
		tailMMap.write(b);
	}
	
	private Path filePath;
	private void open(File dbFile) throws IOException 
	{
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
		
		if (useMemMappedIO) {
			initMemoryMaps();
		}
		closed = false;
		filePath = dbFile.toPath();
	}
	private MMapHeadSeq headMMap;
	private MMapTailSeq tailMMap;
	
	/**
	 * Initialize the memory mapped files corresponding to head and tail regions.
	 * @throws IOException
	 */
	private void initMemoryMaps() throws IOException {
		if (useMemMappedIO) {
			throw new UnsupportedOperationException("Memory mapped IO is an experimental feature");
		}
		headMMap = new MMapHeadSeq(this);
		tailMMap = new MMapTailSeq(this);
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
	private void syncFinally()
	{
		if (syncThread != null) {
			syncThread.shutdown();
			try {
				syncThread.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {

			} 
		}
		try {
			sync(true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	@Override
	public void close() throws IOException {
		
		if (fileLock != null) {
			fileLock.release();
		}
		if (dataFile != null) 
		{
			syncFinally();
			
			if (useMemMappedIO) {
				if (headMMap != null)
					headMMap.close();
				if (tailMMap != null)
					tailMMap.close();
			}
			dataFile.close();
		}
		fileLock = null;
		dataFile = null;
		closed = true;
		
		
	}
	final Object mmapMutex = new Object();
	private volatile boolean closed = false;
	private void sync(boolean force) throws SyncFailedException, IOException
	{
		if (force) {
			dataFile.getFD().sync();
			isDirty.compareAndSet(true, false);
		}
		else
		{
			if(isDirty.compareAndSet(true, false))
				dataFile.getFD().sync();
		}
	}
	public boolean delete()
	{
		try {
			if (!closed) {
				close();
			}
			Files.delete(filePath);
			//System.out.println("QueuedFile.delete()");
		} catch (IOException e) {
			// ignored
			e.printStackTrace();
			return false;
		}
		return true;
		
	}
	public static void main(String[] args) throws IOException {
		
		final int ITERATION = 3000;
		final String msg = "MSG-";
		
		final FileBackedQueue qf = new FileBackedQueue(".", "test_file", true, true);
		try {
			
			System.out.println("qf.isEmpty() ? "+qf.isEmpty());
			System.out.println("qf.size() "+qf.size());
			
			//get
			Thread conThread = new Thread(){
				public void run()
				{
					System.out.println("starting consume for size "+qf.size());
					long s = System.currentTimeMillis();
					while(!qf.isEmpty()){
						try 
						{
							byte[] b = qf.getHead();
							System.out.println(new String(b));
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					long e = System.currentTimeMillis();
					System.out.println("consume Time taken: "+(e-s)+" after consume Size "+qf.size());
				}
			};
			
			AtomicInteger i = new AtomicInteger();
			ExecutorService exec = Executors.newFixedThreadPool(2);
			//add
			long s = System.currentTimeMillis();
									
			for (; i.get() < ITERATION;) {
				try {
					qf.addTail((msg+i.getAndIncrement()).getBytes());
				} catch (IOException e) {
					e.printStackTrace();
				}
				
			}
			exec.shutdown();
			try {
				exec.awaitTermination(1, TimeUnit.HOURS);
			} catch (InterruptedException e2) {
				
			}
			long e = System.currentTimeMillis();
			System.out.println("add Time taken: "+(e-s)+" Size "+qf.size());
			
			conThread.start();
			try {
				conThread.join(TimeUnit.SECONDS.toMillis(60));
			} catch (InterruptedException e1) {
				
			}
						
		} finally {
			qf.close();
			if(qf.isEmpty())
				qf.delete();
		}
		
	
	}
}
