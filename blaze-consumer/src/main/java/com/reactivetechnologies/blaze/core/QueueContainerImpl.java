package com.reactivetechnologies.blaze.core;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import com.reactivetechnologies.blaze.handlers.ConsumerRecoveryHandler;
import com.reactivetechnologies.blaze.handlers.DeadLetterHandler;
import com.reactivetechnologies.blaze.ops.RedisDataAccessor;
import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.blaze.throttle.ConsumerThrottlerFactoryBean;
import com.reactivetechnologies.mq.Data;
import com.reactivetechnologies.mq.common.BlazeInternalError;
import com.reactivetechnologies.mq.consume.AbstractQueueListener;
import com.reactivetechnologies.mq.consume.QueueListener;
import com.reactivetechnologies.mq.container.QueueContainer;
/**
 * The core container that manages listener task execution. This class
 * is responsible for scheduling the worker threads amongst the listeners and
 * maintain thread-safety and other concurrency guarantee.
 * @author esutdal
 *
 */
@Component
public class QueueContainerImpl implements Runnable, QueueContainer{

	private static final Logger log = LoggerFactory.getLogger(QueueContainerImpl.class);
	private ExecutorService asyncTasks;
	@Autowired
	private RedisDataAccessor redisOps;
	
	private ExecutorService threadPool;
	@Value("${consumer.worker.thread:0}")
	private int fjWorkers;
	private List<ExecutorService> threadPools;
	private static ForkJoinPool newFJPool(int coreThreads, String name)
	{
		return new ForkJoinPool(coreThreads, new ForkJoinWorkerThreadFactory() {
		      
		      private int containerThreadCount = 0;

			@Override
		      public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
		        ForkJoinWorkerThread t = new ForkJoinWorkerThread(pool){
		          
		        };
		        t.setName(name+"-Worker-"+(containerThreadCount ++));
		        return t;
		      }
		    }, new UncaughtExceptionHandler() {
		      
		      @Override
		      public void uncaughtException(Thread t, Throwable e) {
		        log.error("-- Uncaught exception in fork join pool --", e);
		        
		      }
		    }, true);
	}
		
	@PostConstruct
	void init()
	{
		threadPools = new ArrayList<>();
		int coreThreads =  fjWorkers <= 0 ? Runtime.getRuntime().availableProcessors() : fjWorkers;
		
		threadPool = newFJPool(coreThreads, "BlazeSharedPool");
		threadPools.add(threadPool);
		
		asyncTasks = Executors.newCachedThreadPool(new ThreadFactory() {
			int n=1;
			@Override
			public Thread newThread(Runnable arg0) {
				Thread t = new Thread(arg0, "container-async-thread-"+(n++));
				return t;
			}
		});
		threadPools.add(asyncTasks);
		
		running = true;
		log.info("Container initialized with parallelism "+((ForkJoinPool) threadPool).getParallelism() + ", coreThreads "+coreThreads);
		
		run();
	}
	
	private void shutdownPools()
	{
		for (ExecutorService pool : threadPools) {
			pool.shutdown();
			try {
				pool.awaitTermination(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {

			} 
		}
	}
	@PreDestroy
	public void destroy()
	{
		running = false;
		shutdownPools();
		for(AbstractQueueListener<? extends Data> l : listeners)
		{
			l.destroy();
			log.info("["+l.identifier()+"] consumer destroyed");
		}
		log.info("Container stopped..");
	}
	private final List<AbstractQueueListener<? extends Data>> listeners = Collections.synchronizedList(new ArrayList<>());
		
	private volatile boolean running;
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.IQueueListenerContainer#register(com.reactivetech.messaging.cmq.core.AbstractQueueListener)
	 */
	@Override
	public <T extends Data> void register(QueueListener<T> listener)
	{
		Assert.isInstanceOf(AbstractQueueListener.class, listener);
		AbstractQueueListener<T> aListener = (AbstractQueueListener<T>) listener;
		register0(aListener);
				
		log.info("* Added listener "+listener);
	}
	private <T extends Data> void register0(AbstractQueueListener<T> aListener)
	{
		listeners.add(aListener);
		run(aListener);
	}
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.IQueueListenerContainer#run()
	 */
	@Override
	public void run() {
		log.debug("running poll task for tasks " + listeners.size());
		for (AbstractQueueListener<? extends Data> task : listeners) {
			run(task);
		}
		
	}
	@Autowired
	private ConsumerThrottlerFactoryBean throttlerFactory;
	
	@Value("${consumer.throttle.tps.millis:1000}")
	private long throttlerPeriod;
	@Value("${consumer.throttle.enable:true}")
	private boolean enabled;
	
	private void initConsumer(AbstractQueueListener<? extends Data> task)
	{
		runRecoveryHandler(task.exchange(), task.routing());
		try {
			task.init();
		} catch (Exception e) {
			throw new BeanInitializationException("Exception on consumer init for task "+task.identifier(), e);
		}
	}
	@Autowired
	private ConsumerRecoveryHandler recoveryHdlr;
	private void runRecoveryHandler(String exchange, String routing) {
		recoveryHdlr.handle(exchange, routing);
	}

	//Consider pool per listener? ForkJoinPool doesn't seem to be efficient 
	//in multiple listener environment. Can there be scenario for a listener
	//starvation?
	/**
	 * Run the specified listener.
	 * @param task
	 */
	private void run(AbstractQueueListener<? extends Data> task) {
		if(running)
		{
			initConsumer(task);
			try 
			{
				if(enabled)
				{
					log.info("Consumer "+task.identifier() + " to be throttled @TPS "+throttleTps);
				}
				QueueContainerTaskImpl<? extends Data> runnable = new QueueContainerTaskImpl<>(task, this, throttlerFactory.getObject(throttlerPeriod, enabled));
				runnable.setThrottleTps(throttleTps);
				log.debug("SUBMITTING TASK FOR ------------------- "+task);
				if(task.useSharedPool())
					((ForkJoinPool) threadPool).execute(runnable);
				else
				{
					String name = task.identifier().length() > 20 ? task.identifier().substring(0, 20) : task.identifier();
					ForkJoinPool pool = newFJPool(Runtime.getRuntime().availableProcessors(), name);
					pool.execute(runnable);
					threadPools.add(pool);
				}
				
			} 
			catch (Exception e) {
				throw new BlazeInternalError("", e);
			}
		}
		
	}
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.IQueueListenerContainer#commit(com.reactivetech.messaging.cmq.dao.QRecord, boolean)
	 */
	@Override
	public void commit(QRecord qr, boolean success) {
		String preparedKey = RedisDataAccessor.prepareListKey(qr.getKey().getExchange(), qr.getKey().getRoutingKey());
		//this can be made async
		asyncTasks.submit(new Runnable() {
			@Override
			public void run() {
				//redisOps.endCommit(qr, preparedKey);
				redisOps.endCommit(qr, preparedKey, false);
			}
		});
		
		if(!success)
		{
			//message being lost
			recordDeadLetter(qr);
		}
	}
	@Autowired
	private DeadLetterHandler deadLetterService;
	/**
	 * TODO: Handle messages discarded after retry limit exceeded or expiration or unknown cause.
	 * @param qr
	 */
	protected void recordDeadLetter(QRecord qr) {
		deadLetterService.handle(qr);
	}
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.IQueueListenerContainer#rollback(com.reactivetech.messaging.cmq.dao.QRecord)
	 */
	@Override
	public void rollback(QRecord qr) {
		String preparedKey = RedisDataAccessor.prepareListKey(qr.getKey().getExchange(), qr.getKey().getRoutingKey());
		//redisOps.endCommit(qr, preparedKey);
		//redisOps.reEnqueue(qr, preparedKey);
		redisOps.endCommit(qr, preparedKey, true);
	}
	@Value("${consumer.poll.await.millis:100}")
	private long pollInterval;

	@Value("${consumer.throttle.tps:1000}")
	private int throttleTps;
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.IQueueListenerContainer#getPollInterval()
	 */
	@Override
	public long getPollInterval() {
		return pollInterval;
	}
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.IQueueListenerContainer#setPollInterval(long)
	 */
	@Override
	public void setPollInterval(long pollInterval) {
		this.pollInterval = pollInterval;
	}
	/**
	 * Fetch head.
	 * @param exchange
	 * @param routing
	 * @param pollInterval2
	 * @param milliseconds
	 * @return
	 * @throws TimeoutException
	 */
	QRecord fetchHead(String exchange, String routing, long pollInterval2, TimeUnit milliseconds) throws TimeoutException
	{
		QRecord qr = redisOps.dequeue(exchange, routing, pollInterval2, milliseconds);
		return qr;
	}
}
