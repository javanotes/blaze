package com.reactivetechnologies.blaze.api;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.reactivetechnologies.blaze.ops.ProducerDataAccessor;
//import com.reactivetechnologies.blaze.ops.RedisDataAccessor;
import com.reactivetechnologies.blaze.ops.RedisStatsRecorder;
import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.mq.Data;
import com.reactivetechnologies.mq.MetricService;
import com.reactivetechnologies.mq.QueueService;

@Service
@Qualifier("RMQ")
public class QueueServiceImpl implements QueueService, MetricService{

	private static final Logger log = LoggerFactory.getLogger(QueueServiceImpl.class);
	
	@Autowired
	private ProducerDataAccessor producerOps;
	@Autowired
	private RedisStatsRecorder metrics;
	
	@Override
	public long size(String q) {
		return size(QueueService.DEFAULT_XCHANGE, q);
	}

	@Override
	public void clear(String q) {
		clear(QueueService.DEFAULT_XCHANGE, q);
	}

	@Override
	public <T extends Data> int add(List<T> msg, String exchangeKey) {
		Assert.notEmpty(msg);
		Assert.isTrue(StringUtils.hasText(msg.get(0).getDestination()), "Destination not provided");
		return add0(msg, exchangeKey, msg.get(0).getDestination(), true);
	}

	private String prepareKey(String xchangeKey, String routeKey)
	{
		return producerOps.prepareListKey(xchangeKey, routeKey);
	}
	
	private <T extends Data> int add0(List<T> msg, String xchangeKey, String routeKey, boolean getcount) 
	{
		QRecord qr;
		long start = System.currentTimeMillis();
		if (log.isDebugEnabled()) {
			start = System.currentTimeMillis();
			log.debug(">>> ingestEntitiesAsync: Starting ingestion batch <<<");
		}
		QRecord[] records = new QRecord[msg.size()];
		int i = 0;
		for (Data t : msg) 
		{
			qr = QRecord.transformData(t, xchangeKey, routeKey);
			records[i++] = qr;
		}
		producerOps.enqueue(prepareKey(xchangeKey, routeKey), records);
		
		long time = System.currentTimeMillis() - start;
		long secs = TimeUnit.MILLISECONDS.toSeconds(time);
		log.info(i+" items pushed. Time taken: " + secs + " secs " + (time - TimeUnit.SECONDS.toMillis(secs)) + " ms");

		return i;
	}
	
	@Override
	public long size(String xchangeKey, String routeKey) {
		return producerOps.size(xchangeKey, routeKey);
	}

	public static final int MAX_COMPARE_ON_CLEAR = 5;
	@Override
	public void clear(String xchangeKey, String routeKey) {
		log.info("'clear' is an expensive operation since Redis does not provide an explicit operation");
		boolean cleared = false;
		int iter = 0;
		do {
			cleared = producerOps.clear(xchangeKey, routeKey);
		} while (++iter < MAX_COMPARE_ON_CLEAR && !cleared);
		
		if(!cleared)
		{
			log.warn("clear: Breaking circuit after iterating for max "+MAX_COMPARE_ON_CLEAR+" times");
		}
	}

	@Override
	public <T extends Data> int add(List<T> msg) {
		Assert.notEmpty(msg);
		Assert.isTrue(StringUtils.hasText(msg.get(0).getDestination()), "Destination not provided");
		return add0(msg, DEFAULT_XCHANGE, msg.get(0).getDestination(), true);
	}

	@Override
	public <T extends Data> void ingest(List<T> msg) {
		Assert.notEmpty(msg);
		Assert.isTrue(StringUtils.hasText(msg.get(0).getDestination()), "Destination not provided");
		add0(msg, DEFAULT_XCHANGE, msg.get(0).getDestination(), false);
		
	}

	@Override
	public <T extends Data> void ingest(List<T> msg, String xchangeKey) {
		Assert.notEmpty(msg);
		Assert.isTrue(StringUtils.hasText(msg.get(0).getDestination()), "Destination not provided");
		add0(msg, xchangeKey, msg.get(0).getDestination(), false);
	}

	/*@Override
	public QRecord getNext(String xchng, String route, long timeout, TimeUnit unit) {
		return redisOps.pop(xchng, route, timeout, unit);
	}*/

	@Override
	public long getDequeueCount(String exchange, String route) {
		return metrics.getDequeuStats(prepareKey(exchange, route));
	}

	@Override
	public long getEnqueueCount(String exchange, String route) {
		return metrics.getEnqueuStats(prepareKey(exchange, route));
	}

	@Override
	public void resetCounts(String exchange, String route) {
		metrics.reset(prepareKey(exchange, route));
	}

	@Override
	public long getDequeueCount(String route) {
		return getDequeueCount(DEFAULT_XCHANGE, route);
	}

	@Override
	public long getEnqueueCount(String route) {
		return getEnqueueCount(DEFAULT_XCHANGE, route);
	}

	@Override
	public void resetCounts(String route) {
		resetCounts(DEFAULT_XCHANGE, route);
	}

}
