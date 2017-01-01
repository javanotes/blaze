package com.reactivetech.messaging;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.reactivetechnologies.blaze.BlazeConsumer;
import com.reactivetechnologies.mq.QueueService;
import com.reactivetechnologies.mq.consume.Consumer;
import com.reactivetechnologies.mq.consume.QueueListener;
import com.reactivetechnologies.mq.consume.QueueListenerBuilder;
import com.reactivetechnologies.mq.container.QueueContainer;
import com.reactivetechnologies.mq.data.TextData;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {BlazeConsumer.class})
public class RedisQueueListenerReDeliveryTest {

	@Autowired
	QueueService service;
	@Autowired
	QueueContainer container;
	
	static final Logger log = LoggerFactory.getLogger(RedisQueueListenerReDeliveryTest.class);
	static String QNAME = SimpleQueueListener.QNAME;
	static String PAYLOAD = "Message9.0x";
	@Before
	public void publish()
	{
		service.clear(QNAME);
		Assert.assertEquals(0, service.size(QNAME));
		service.add(Arrays.asList(new TextData(PAYLOAD, QNAME)));
		Assert.assertEquals(1, service.size(QNAME));
	}
	@After
	public void checkDeadLettered()
	{
		long llen = service.size(QNAME);
		Assert.assertEquals(0, llen);
	}
	@Test
	public void testWithRedelivery()
	{
		
		CountDownLatch l = new CountDownLatch(3);
		QueueListener<TextData> abs = new QueueListenerBuilder()
		.concurrency(1)
		//.maxDelivery((short) 3)
		.consumer(new Consumer<TextData>() {

			@Override
			public void onMessage(TextData m) throws Exception {
				log.info("Recieved message ... " + m.getPayload());
				l.countDown();
				throw new IllegalArgumentException("Dummy exception raised");
				
			}

			@Override
			public void destroy() {
				// TODO Auto-generated method stub
				
			}

			@Override
			public void init() {
				// TODO Auto-generated method stub
				
			}
		})
		.route(QNAME)
		.dataType(TextData.class)
		.build();
		
		long start = System.currentTimeMillis();
		container.register(abs);
		
		try {
			boolean b = l.await(10, TimeUnit.SECONDS);
			Assert.assertTrue(b);
		} catch (InterruptedException e) {
			
		}
		
		long time = System.currentTimeMillis() - start;
		long secs = TimeUnit.MILLISECONDS.toSeconds(time);
		log.info("Time taken: " + secs + " secs " + (time - TimeUnit.SECONDS.toMillis(secs)) + " ms");
	}
}
