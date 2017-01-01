package com.reactivetech.messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.reactivetechnologies.blaze.BlazeWeb;
import com.reactivetechnologies.mq.MetricService;
import com.reactivetechnologies.mq.QueueService;
import com.reactivetechnologies.mq.data.TextData;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {BlazeWeb.class})
public class RedisQueueServiceIngestTest {

	@Autowired
	QueueService service;
	@Autowired
	MetricService metrics;
	private final int iteration = 100;
	
	@Before
	public void pre()
	{
		metrics.resetCounts(SimpleQueueListener.QNAME);
	}
	@Test
	public void testAddToQueue()
	{
		UUID u = UUID.randomUUID();
		TextData sm = null;
		List<TextData> m = new ArrayList<>();
		for(int i=0; i<iteration; i++)
		{
			sm = new TextData("HELLOCMQ "+i, SimpleQueueListener.QNAME, u.toString());
			m.add(sm);
		}
		try {
			service.ingest(m);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		
		Assert.assertEquals(iteration, metrics.getEnqueueCount(SimpleQueueListener.QNAME));
		Assert.assertEquals(0, metrics.getDequeueCount(SimpleQueueListener.QNAME));
		Assert.assertEquals(iteration, service.size(SimpleQueueListener.QNAME));
	}
	
}
