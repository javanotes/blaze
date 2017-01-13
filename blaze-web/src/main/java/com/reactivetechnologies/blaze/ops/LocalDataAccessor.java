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
package com.reactivetechnologies.blaze.ops;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.mq.disk.LocalDurableQueue;
import com.reactivetechnologies.mq.ops.ProducerOperations;

class LocalDataAccessor implements Closeable{

	private static final Logger log = LoggerFactory.getLogger(LocalDataAccessor.class);
	/**
	 * 
	 * @param localQueueDir
	 */
	public LocalDataAccessor(String localQueueDir) 
	{
		this(localQueueDir, System.currentTimeMillis()+"");
	}
	/**
	 * 
	 * @param localQueueDir
	 * @param fileNamePattern
	 */
	private LocalDataAccessor(String localQueueDir, String fileNamePattern) {
		this.localQueueDir = localQueueDir;
		localQueue = new LocalDurableQueue<>(QRecordExternalizable.class, fileNamePattern, this.localQueueDir);
		log.debug("Local queue file: "+localQueue.getPath());
	}
	private final String localQueueDir;
	/**
	 * 
	 * @param qr
	 * @param preparedKey
	 */
	private void enqueueLocally(QRecord qr, String preparedKey) {
		QRecordExternalizable ext = new QRecordExternalizable(qr);
		ext.setPreparedKey(preparedKey);
		localQueue.add(ext);
	}
	/**
	 * This method will poll and delete the file
	 * @return
	 */
	private List<QRecordExternalizable> pollLocal() {
		List<QRecordExternalizable> localList = new ArrayList<>();
		while(!localQueue.isEmpty())
		{
			QRecordExternalizable qrec = localQueue.poll();
			localList.add(qrec);
		}
		
		return localList;
	}
	/**
	 * Delete the local file.
	 */
	void destroy()
	{
		localQueue.destroy();
		
	}
	private LocalDurableQueue<QRecordExternalizable> localQueue;
	
	private static Set<String> arrangeFilesInChronologicalOrder(String[] files)
	{
		Set<String> fileOrd = new TreeSet<>(new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return Long.compare(Long.valueOf(o1), Long.valueOf(o2));
			}
		});
		List<String> fileList = Arrays.asList(files);
		
		fileList.replaceAll(new UnaryOperator<String>() {
			
			@Override
			public String apply(String input) {
				return input.substring(0, input.indexOf('.'));
			}
		});
		fileOrd.addAll(fileList);
		
		return fileOrd;
	}
	
	private void releaseResources()
	{
		if(isOpen())
			try {
				close();
			} catch (IOException e) {
				
			}
		
		destroy();
	}
	/**
	 * Move all locally queued files in the current directory to Redis. Note, this
	 * instance will become unusable after this method returns successfully.
	 * @param redisOps
	 */
	public void moveAll(ProducerOperations redisOps) {
		Assert.isTrue(!closed, "Already closed");
		String[] files = localQueue.listFilesInDir();
		if(files != null)
		{
			//we are closing this file because we would now scan for all files 
			//present in this directory (which will include this file as well)
			//iteratively to PUSH to Redis
			try {
				close();
			} catch (IOException e) {
				
			}
			for(String filePattern : arrangeFilesInChronologicalOrder(files))
			{
				log.info("Moving file- "+filePattern);
				try(LocalDataAccessor localDataFile = new LocalDataAccessor(localQueueDir, filePattern))
				{
					if (!localDataFile.isEmpty()) 
					{
						List<QRecordExternalizable> records = localDataFile.pollLocal();
						boolean moved = move(records, redisOps);
						if (moved) {
							log.debug("Moved file- " + filePattern);
							localDataFile.releaseResources();
						} else {
							log.warn("File " + filePattern
									+ " was not moved! Probably it is corrupted. This is an irrecoverable situation");
						} 
					}
					else
					{
						log.debug("Removed file- " + filePattern);
						localDataFile.releaseResources();
					}
				} 
				catch (IOException e) {
					log.error("While moving local file", e);
				}
			}
		}
		else
		{
			log.info("No files were found to be moved..");
		}
	}
	/**
	 * Will move (or ignore) files and return true. Will return false if the file is corrupted.
	 * The only reason it will be considered corrupted if the key was not present.
	 * @param records
	 * @param redisOps
	 * @return
	 */
	private boolean move(List<QRecordExternalizable> records, ProducerOperations redisOps) {
		log.debug("List<QRecordExternalizable> records.size "+records.size());
		if (records != null && !records.isEmpty()) 
		{
			String preparedKey = null;
			QRecord[] items = new QRecord[records.size()];
			int i = 0;
			for (QRecordExternalizable record : records) {
				items[i++] = record.getRecord();
				preparedKey = record.getPreparedKey();
			}
			log.debug("preparedKey "+preparedKey);
			if (StringUtils.hasText(preparedKey)) {
				redisOps.lpushAll(preparedKey, items);
				return true;
			} 
			else
			{
				//records present but key not found
				return false;
			}
		}
		//there was no data to be moved. so considering this as moved
		return true;
	}
	/**
	 * Dump items to local file system.
	 * @param preparedKey
	 * @param values
	 */
	public void addAll(String preparedKey, QRecord[] values) {
		Assert.isTrue(!closed, "Already closed");
		for(QRecord qr : values)
			enqueueLocally(qr, preparedKey);
	}
	private volatile boolean closed = false;
	@Override
	public void close() throws IOException {
		localQueue.close();
		closed = true;
	}
	public boolean isOpen() {
		return !closed;
	}
	public boolean isEmpty() {
		return localQueue.isEmpty();
	}
	
	public static void main(String[] args) {/*
		LocalDataAccessor ld = new LocalDataAccessor(".");
		try {
			String key = "key";
			ld.enqueueLocally(QRecord.transformData(new TextData("hello1", "helloQ", "corrID")), key);
			System.out.println("enqueue ");
			ld.enqueueLocally(QRecord.transformData(new TextData("hello2", "helloQ", "corrID")), key);
			System.out.println("enqueue ");
			ld.enqueueLocally(QRecord.transformData(new TextData("hello3", "helloQ", "corrID")), key);
			System.out.println("enqueue ");
			
			List<QRecordExternalizable> qe = ld.pollLocal();
			System.out.println(qe.size());
			System.out.println(qe);
			
		} finally {
			ld.releaseResources();
		}
	*/}
}
