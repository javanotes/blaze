package com.reactivetechnologies.blaze.api;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.BoundListOperations;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.reactivetechnologies.blaze.ops.RedisDataAccessor;
import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.mq.Data;
import com.reactivetechnologies.mq.QueueService;

@Service
@Qualifier("RMQ")
public class QueueServiceImpl implements QueueService{

	private static final Logger log = LoggerFactory.getLogger(QueueServiceImpl.class);
	
	@Autowired
	private RedisDataAccessor redisOps;
	
	@Override
	public Integer size(String q) {
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

	private static String prepareKey(String xchangeKey, String routeKey)
	{
		return RedisDataAccessor.prepareListKey(xchangeKey, routeKey);
	}
	
	private static QRecord dataToRecord(Data t, String xchangeKey, String routeKey)
	{
		QRecord qr = new QRecord(t);
		qr.getKey().setExchange(xchangeKey);
		qr.getKey().setRoutingKey(routeKey);
		qr.getKey().setTimeuid(UUID.randomUUID());
		qr.setT0TS(new Date());
		
		return qr;
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
			qr = dataToRecord(t, xchangeKey, routeKey);
			records[i++] = qr;
		}
		redisOps.enqueue(prepareKey(xchangeKey, routeKey), records);
		
		long time = System.currentTimeMillis() - start;
		long secs = TimeUnit.MILLISECONDS.toSeconds(time);
		log.info(i+" items pushed. Time taken: " + secs + " secs " + (time - TimeUnit.SECONDS.toMillis(secs)) + " ms");

		return i;
	}
	private BoundListOperations<String, QRecord> listOperations(String xchangeKey, String routeKey)
	{
		return redisOps.boundListOps(prepareKey(xchangeKey, routeKey));
	}
	@Override
	public Integer size(String xchangeKey, String routeKey) {
		BoundListOperations<String, QRecord> listOps = listOperations(xchangeKey, routeKey);
		return listOps.size().intValue();
	}

	@Override
	public void clear(String xchangeKey, String routeKey) {
		log.warn("'clear' is an expensive operation since Redis does not provide an explicit operation");
		long llen = size(xchangeKey, routeKey);
		if(llen > 0)
		{
			redisOps.executePipelined(new RedisCallback<Integer>() {

				@Override
				public Integer doInRedis(RedisConnection connection) throws DataAccessException {
					StringRedisSerializer ser = redisOps.getKeySerializer();
					byte[] key = ser.serialize(prepareKey(xchangeKey, routeKey));
					for(long l=0; l<llen; l++)
					{
						connection.lPop(key);
					}
					return null;
				}
			});
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

	@Override
	public QRecord getNext(String xchng, String route, long timeout, TimeUnit unit) {
		return redisOps.dequeue(xchng, route, timeout, unit);
	}

}
