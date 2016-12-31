package com.reactivetech.messaging;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactivetechnologies.mq.consume.AbstractQueueListener;
import com.reactivetechnologies.mq.data.TextData;

public class SimpleQueueListener extends AbstractQueueListener<TextData> {

	static final String QNAME = "TEST-QUEUE";
	static final Logger log = LoggerFactory.getLogger(SimpleQueueListener.class);
	@Override
	public Class<TextData> dataType() {
		return TextData.class;
	}

	@Override
	public void onMessage(TextData m) throws Exception {
		log.info("Recieved message ... "+m.getPayload());
		log.info("CorrId ... "+m.getCorrelationID());
	}

	public int concurrency() {
		return 8;
	}
	@Override
	public String routing() {
		return QNAME;
	}

	@Override
	public void init() {
		// TODO Auto-generated method stub
		
	}

}
