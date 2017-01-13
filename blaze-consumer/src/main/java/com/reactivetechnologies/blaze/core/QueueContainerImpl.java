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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

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
import com.reactivetechnologies.blaze.ops.ConsumerDataAccessor;
import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.blaze.throttle.ConsumerThrottlerFactoryBean;
import com.reactivetechnologies.mq.Data;
import com.reactivetechnologies.mq.consume.AbstractQueueListener;
import com.reactivetechnologies.mq.consume.QueueListener;
import com.reactivetechnologies.mq.container.QueueContainer;
import com.reactivetechnologies.mq.exceptions.BlazeInternalException;
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
	private ScheduledExecutorService scheduledTasks;
	@Autowired
	private ConsumerDataAccessor redisOps;
	
	private ExecutorService threadPool;
	@Value("${consumer.worker.thread:0}")
	private int fjWorkers;
	@Value("${consumer.redelivery.delay.millis:1000}")
	private long backoffRollbackDelay;
	@Value("${consumer.redelivery.delay.backoffExp:0}")
	private int backoffRollbackExponent;
	private List<ExecutorService> threadPools;
	private static ForkJoinPool newFJPool(int coreThreads, String name)
	{
		return new ForkJoinPool(coreThreads, new ForkJoinWorkerThreadFactory() {
		      
		      private int containerThreadCount = 0;

			@Override
		      public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
		        ForkJoinWorkerThread t = new ForkJoinWorkerThread(pool){
		          
		        };
		        t.setName(name+".Worker."+(containerThreadCount ++));
		        return t;
		      }
		    }, new UncaughtExceptionHandler() {
		      
		      @Override
		      public void uncaughtException(Thread t, Throwable e) {
		        log.error("-- Uncaught exception in fork join pool --", e);
		        
		      }
		    }, true);
	}
	/**
	 * Perform a backing off rollback (exponentially if required), so that the message
	 * gets (re)delivered only after a delay. This is done in message oriented middle-wares 
	 * to allow some time for recovery at consumer end, if possible.
	 * @param qr
	 */
	final void scheduleRollback(QRecord qr)
	{
		if(backoffRollbackDelay > 0){
			long delay = backoffRollbackDelay + (qr.getRedeliveryCount() * backoffRollbackExponent);
			log.info("Backing off redlivery by "+delay+" millis");
			
			scheduleTaskAfter(new Runnable() {
				
				@Override
				public void run() {
					rollback(qr);
				}
			}, delay, TimeUnit.MILLISECONDS);
		}
		else
			rollback(qr);
	}
	/**
	 * Create and execute a one-shot action that becomes enabled after the given delay.	
	 * @param task
	 * @param delay
	 * @param unit
	 * @return
	 */
	ScheduledFuture<?> scheduleTaskAfter(Runnable task, long delay, TimeUnit unit)
	{
		return scheduledTasks.schedule(task, delay, unit);
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
				Thread t = new Thread(arg0, "BlazeContainerTask."+(n++));
				return t;
			}
		});
		threadPools.add(asyncTasks);
		
		scheduledTasks = Executors.newScheduledThreadPool(coreThreads, new ThreadFactory() {
			int n=1;
			@Override
			public Thread newThread(Runnable arg0) {
				Thread t = new Thread(arg0, "BlazeContainerScheduler."+(n++));
				t.setDaemon(true);
				return t;
			}
		});
		threadPools.add(scheduledTasks);
		
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
			recordToStats(l, false);
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
		recordToStats(aListener, true);
	}
	private <T extends Data> void recordToStats(AbstractQueueListener<T> aListener, boolean isRegistered) {
		// TODO recordToStats consumer
		
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

	private BlazeQueueIterator newQueueIterator(AbstractQueueListener<? extends Data> task) throws Exception
	{
		BlazeQueueIterator iter = new BlazeQueueIterator(throttlerFactory.getObject(throttlerPeriod, enabled), throttleTps, redisOps);
		iter.setExchange(task.exchange());
		iter.setRouting(task.routing());
		iter.setPollIntervalMillis(getPollInterval());
		
		return iter;
	}
	private QueueContainerTaskImpl<? extends Data> prepareTask(AbstractQueueListener<? extends Data> task) throws Exception
	{
		BlazeQueueIterator iter = newQueueIterator(task);
		QueueContainerTaskImpl<? extends Data> runnable = new QueueContainerTaskImpl<>(task, this, iter);
		
		return runnable;
	}
	/**
	 * This will create dedicated fork-join pools for each consumer.
	 * @param task
	 * @param runnable
	 */
	private void executeInNewPool(AbstractQueueListener<? extends Data> task, QueueContainerTaskImpl<? extends Data> runnable)
	{
		//CAVEAT: On a test laptop with 4 core processors, it was found that multiple fork-join pools in the same jvm
		//is not a good idea and the work stealing approach did not scale in this case. Worst still, it was
		//found that threads across pool instances were getting starved, and not getting a chance to run at all!
		//This is why the default mode is to use a shared pool, and from application perspective we would suggest
		//to consider the framework as a lightweight micro-container for single consumer per jvm.
		
		String name = task.identifier().length() > 20 ? task.identifier().substring(0, 20) : task.identifier();
		ForkJoinPool pool = newFJPool(Runtime.getRuntime().availableProcessors(), name);
		pool.execute(runnable);
		threadPools.add(pool);
	}
	private void execute(AbstractQueueListener<? extends Data> task) throws Exception
	{
		QueueContainerTaskImpl<? extends Data> runnable = prepareTask(task);
		log.debug("SUBMITTING TASK FOR ------------------- "+task);
		if(task.useSharedPool())
			((ForkJoinPool) threadPool).execute(runnable);
		else
		{
			executeInNewPool(task, runnable);
		}
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

				execute(task);
				
			} 
			catch (Exception e) {
				throw new BlazeInternalException("", e);
			}
		}
		
	}
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.IQueueListenerContainer#commit(com.reactivetech.messaging.cmq.dao.QRecord, boolean)
	 */
	@Override
	public void commit(QRecord qr, boolean success) {
		String preparedKey = redisOps.prepareListKey(qr.getKey().getExchange(), qr.getKey().getRoutingKey());
		redisOps.endCommit(qr, preparedKey, false);
		if(!success)
		{
			asyncTasks.submit(new Runnable() {
				
				@Override
				public void run() {
					//message being lost
					recordDeadLetter(qr);
				}
			});
			
		}
	}
	@Autowired
	private DeadLetterHandler deadLetterService;
	/**
	 * Handle messages discarded after retry limit exceeded or expiration or unknown cause.
	 * @param qr
	 */
	private void recordDeadLetter(QRecord qr) {
		deadLetterService.handle(qr);
	}
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.IQueueListenerContainer#rollback(com.reactivetech.messaging.cmq.dao.QRecord)
	 */
	@Override
	public void rollback(QRecord qr) {
		String preparedKey = redisOps.prepareListKey(qr.getKey().getExchange(), qr.getKey().getRoutingKey());
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
	
}
