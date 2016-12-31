package com.reactivetech.messaging;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.reactivetechnologies.blaze.BlazeApi;
import com.reactivetechnologies.mq.QueueService;
import com.reactivetechnologies.mq.data.TextData;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = {BlazeApi.class})
public class RedisQueueServiceIngestTest {

	@Autowired
	QueueService service;
	
	private final int iteration = 100;
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
			Assert.fail(e.getMessage());
		}
	}
	
}
