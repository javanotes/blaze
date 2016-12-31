package com.reactivetechnologies.mq.consume;

import com.reactivetechnologies.mq.Data;
import com.reactivetechnologies.mq.container.QueueContainer;

/**
 * Callback on a CMQ. Do NOT use this interface directly. Subclass from {@linkplain AbstractQueueListener}.
 * @author esutdal
 * @see AbstractQueueListener
 * @see QueueContainer#register(QueueListener)
 * @param <T>
 */
public interface QueueListener<T extends Data> extends Consumer<T> {

	/**
	 * The type of {@linkplain Data} this listener is receiving.
	 * @return
	 */
	Class<T> dataType();
	/**
	 * Unique identifier for this queue listener
	 * @return
	 */
	String identifier();
	/**
	 * Max parallelism to achieve
	 * @return
	 */
	int concurrency();
	/**
	 * The exchange type
	 * @return
	 */
	String exchange();
	/**
	 * The routing key (queue name) for the given exchange.
	 * @return
	 */
	String routing();
	/**
	 * Max redelivery attempts.
	 * @return
	 */
	short maxDeliveryAttempts();
		
	/**
	 * Check if a record is eligible for re-delivery based on expiration/delivery count. The {@linkplain Data} instance
	 * may be null.
	 * @param expired
	 * @param redeliveryCount
	 * @param d
	 * @return
	 */
	boolean allowRedelivery(boolean expired, short redeliveryCount, Data d);
	/**
	 * Exception handler for listeners. The handler is called back only
	 * when a message is going to be redelivered due to an execution exception at {@link #onMessage(Data)}.
	 * @param e
	 * @param d
	 */
	void onExceptionCaught(Throwable e, Data d);

}