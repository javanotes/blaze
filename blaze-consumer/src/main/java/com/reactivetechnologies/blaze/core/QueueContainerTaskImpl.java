package com.reactivetechnologies.blaze.core;

import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.blaze.throttle.ConsumerThrottler;
import com.reactivetechnologies.mq.Data;
import com.reactivetechnologies.mq.common.BlazeMessagingException;
import com.reactivetechnologies.mq.common.MessageThrottledException;
import com.reactivetechnologies.mq.consume.AbstractQueueListener;
import com.reactivetechnologies.mq.container.QueueContainerTask;
/**
 * The task class that works in a work-stealing thread pool.
 * @author esutdal
 *
 */
class QueueContainerTaskImpl<T extends Data> extends RecursiveAction implements QueueContainerTask
{
	private static final Logger log = LoggerFactory.getLogger(QueueContainerTaskImpl.class);
	private final int concurrency;
	private final AbstractQueueListener<T> consumer;
	private final QueueContainerImpl container;
	private final ConsumerThrottler throttler;
	private int throttleTps;
	public int getThrottleTps() {
		return throttleTps;
	}
	public void setThrottleTps(int throttleTps) {
		this.throttleTps = throttleTps;
	}
	/**
	 * Instantiates a new task with concurrency level as set in the consumer. This constructor is kept
	 * public to schedule the first shot of task from the container.
	 * @param <T>
	 * @param ql
	 */
	public QueueContainerTaskImpl(AbstractQueueListener<T> ql, QueueContainerImpl container, ConsumerThrottler throttler) {
		this(ql, ql.concurrency(), container, throttler);
	}
	/**
	 * Fork new parallel tasks to be scheduled in a work stealing pool. This constructor will be invoked from within
	 * the {@linkplain RecursiveAction} compute, to fork new tasks.
	 * @param ql
	 * @param concurrency
	 */
	private QueueContainerTaskImpl(AbstractQueueListener<T> ql, int concurrency, QueueContainerImpl container, ConsumerThrottler throttler) {
		this.concurrency = concurrency;
		this.consumer = ql;
		this.container = container;
		this.throttler = throttler;
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5690149379909633509L;

	@Override
	protected final void compute() {
		log.debug("Fetching next record..compute");
		if (concurrency > 1) 
		{
			forkTasks(concurrency);
		} 
		else 
		{
			run();
		}

	}
	private QueueContainerTaskImpl<T> copy()
	{
		QueueContainerTaskImpl<T> b = new QueueContainerTaskImpl<T>(consumer, 1, container, throttler);
		b.setThrottleTps(this.throttleTps);
		return b;
	}
	/**
	 * Fork parallel tasks based on the concurrency. 
	 * These tasks should be available for work-stealing via FJpool.
	 * @param parallelism
	 */
	private void forkTasks(int parallelism)
	{
		QueueContainerTaskImpl<T> qt = null;
		for(int i=0; i<parallelism; i++)
        {
        	qt = copy();
        	qt.fork();
        }
	}
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.QueueContainerTask#fireOnMessage(com.reactivetech.messaging.cmq.dao.QRecord)
	 */
	@Override
	public void fireOnMessage(QRecord qr)
	{
		try 
		{
			consumer.fireOnMessage(qr);
			container.commit(qr, true);
		}  
		catch(Exception e)
		{
			handleException(qr, e);
			
		}
	}
	/**
	 * 
	 * @param qr
	 * @param e
	 */
	private void handleException(QRecord qr, Exception e)
	{
		Data d = null;
		qr.incrDeliveryCount();
		if(e instanceof BlazeMessagingException)
		{
			d = ((BlazeMessagingException) e).getRecord();
		}
		if(allowRedelivery(qr, d))
		{
			executeRollback(qr, e, d);
		}
		else
		{
			log.error("* MESSAGE BEING DISCARDED. Check stacktrace for root cause.", e);
			container.commit(qr, false);
		}
	}
	/**
	 * 
	 * @param qr
	 * @param d
	 * @return
	 */
	private boolean allowRedelivery(QRecord qr, Data d)
	{
		return consumer.allowRedelivery(qr.isExpired(), qr.getRedeliveryCount(), d);
		
	}
	/**
	 * 
	 * @param qr
	 * @param e
	 * @param d 
	 */
	private void executeRollback(QRecord qr, Throwable e, Data d)
	{
		log.warn("Queue container caught error. Message will be redelivered. Error => "+e.getCause());
		log.debug("", e);
		container.rollback(qr);
		if(d != null)
		{
			consumer.onExceptionCaught(e, d);
		}
		
	}
	/**
	 * Fetch head if available.
	 */
	private void run() 
	{
		log.debug("Fetching next record..");
		try 
		{
			QRecord nextMessage = fetchHead();
			log.debug(nextMessage + "");
			if (nextMessage != null) {
				fireOnMessage(nextMessage);
			} 
			 
		}
		catch (TimeoutException t) {
			log.debug(t+"");
		} 
		catch (MessageThrottledException e) 
		{
			log.debug(e+"");
		}
		catch(Exception e)
		{
			log.error("Unexpected error!", e);
		}
		finally 
		{
			forkTasks(1);//fork next corresponding task
		}
	}
	/* (non-Javadoc)
	 * @see com.reactivetech.messaging.cmq.core.QueueContainerTask#fetchHead()
	 */
	@Override
	public QRecord fetchHead() throws TimeoutException, MessageThrottledException
	{
		return fetchHeadIfNotThrottled();
	}
	
	private QRecord fetchHeadIfNotThrottled() throws TimeoutException, MessageThrottledException {
		if(throttler.allowMessageConsume(throttleTps))
		{
			log.debug("Allowed fetching head");
			QRecord qr = container.fetchHead(consumer.exchange(), consumer.routing(),
					container.getPollInterval(), TimeUnit.MILLISECONDS);
			log.debug("Incrementing count");
			throttler.incrementCount();
			log.debug("Returning..");
			return qr;
		}
		throw new MessageThrottledException();
	}
}
