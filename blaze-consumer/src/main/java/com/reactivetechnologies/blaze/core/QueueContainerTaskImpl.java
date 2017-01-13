package com.reactivetechnologies.blaze.core;

import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.mq.Data;
import com.reactivetechnologies.mq.consume.AbstractQueueListener;
import com.reactivetechnologies.mq.container.QueueContainerTask;
import com.reactivetechnologies.mq.exceptions.BlazeInternalException;
import com.reactivetechnologies.mq.exceptions.BlazeMessagingException;
import com.reactivetechnologies.mq.exceptions.MessageThrottledException;
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
	private final BlazeQueueIterator queueIterator;
	
	/**
	 * Instantiates a new task with concurrency level as set in the consumer. This constructor is kept
	 * public to schedule the first shot of task from the container.
	 * @param <T>
	 * @param ql
	 */
	public QueueContainerTaskImpl(AbstractQueueListener<T> ql, QueueContainerImpl container, BlazeQueueIterator headPopper) {
		this(ql, ql.concurrency(), container, headPopper);
	}
	/**
	 * Fork new parallel tasks to be scheduled in a work stealing pool. This constructor will be invoked from within
	 * the {@linkplain RecursiveAction} compute, to fork new tasks.
	 * @param ql
	 * @param concurrency
	 */
	private QueueContainerTaskImpl(AbstractQueueListener<T> ql, int concurrency, QueueContainerImpl container, BlazeQueueIterator headPopper) {
		this.concurrency = concurrency;
		this.consumer = ql;
		this.container = container;
		this.queueIterator = headPopper;
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5690149379909633509L;

	@Override
	protected final void compute() {
		//log.debug("Fetching next record..compute");
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
		QueueContainerTaskImpl<T> b = new QueueContainerTaskImpl<T>(consumer, 1, container, queueIterator);
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
	protected void fireOnThrottled() {
		// noop
		
	}
	protected void fireOnTimeout() {
		// noop
		
	}
	private void discardMessage(QRecord qr, Throwable e)
	{
		log.error("* MESSAGE BEING DISCARDED. Check stacktrace for root cause.", e);
		container.commit(qr, false);
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
		//delivery count is changing. so equals will fail on redelivery
		//so while doing an endCommit, decrementing the delivery count
		
		if(e instanceof BlazeMessagingException)
		{
			d = ((BlazeMessagingException) e).getRecord();
			if(allowRedelivery(qr, d))
			{
				executeRollback(qr, e, d);
			}
			else
			{
				discardMessage(qr, e);
			}
		}
		else
		{
			discardMessage(qr, e);
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
		//log.debug("Fetching next record..");
		try 
		{
			QRecord nextMessage = fetchHead();
			log.debug(nextMessage + "");
			if (nextMessage != null) {
				fireOnMessage(nextMessage);
			} 
			 
		}
		catch (TimeoutException t) 
		{
			fireOnTimeout();
		} 
		catch (MessageThrottledException e) 
		{
			fireOnThrottled();
		}
		catch(Exception e)
		{
			BlazeInternalException be = new BlazeInternalException("Unexpected error!", e);
			log.error("Internal error: Check stacktrace", be);
			throw be;
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
		if(queueIterator.hasNext())
		{
			QRecord qr = queueIterator.next();
			if(qr == null)
				throw new TimeoutException();
			return qr;
		}
		throw new MessageThrottledException();
	}
	
}
