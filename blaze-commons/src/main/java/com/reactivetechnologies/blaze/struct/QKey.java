package com.reactivetechnologies.blaze.struct;

import java.io.Serializable;
import java.util.UUID;

import com.reactivetechnologies.mq.QueueService;

public class QKey implements Serializable{

	
	@Override
	public String toString() {
		return "QKey [exchange=" + exchange + ", routingKey=" + routingKey + ", timeuid=" + timeuid + "]";
	}
	public QKey() {
		this("", "");
	}
	public QKey(String xchange, String route) {
		setExchange(xchange);
		setRoutingKey(route);
	}
	public QKey(String route) {
		this(QueueService.DEFAULT_XCHANGE, route);
	}
	public QKey copy()
	{
		QKey qk = new QKey(this.exchange, this.routingKey);
		qk.timeuid = this.timeuid;
		return qk;
	}
	/**
	 * 
	 */
	private static final long serialVersionUID = -2787736896327449756L;

	private String exchange = QueueService.DEFAULT_XCHANGE;

	public String getExchange() {
		return exchange;
	}

	public void setExchange(String exchange) {
		this.exchange = exchange;
	}

	public String getRoutingKey() {
		return routingKey;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((exchange == null) ? 0 : exchange.hashCode());
		result = prime * result + ((routingKey == null) ? 0 : routingKey.hashCode());
		result = prime * result + ((timeuid == null) ? 0 : timeuid.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QKey other = (QKey) obj;
		if (exchange == null) {
			if (other.exchange != null)
				return false;
		} else if (!exchange.equals(other.exchange))
			return false;
		if (routingKey == null) {
			if (other.routingKey != null)
				return false;
		} else if (!routingKey.equals(other.routingKey))
			return false;
		if (timeuid == null) {
			if (other.timeuid != null)
				return false;
		} else if (!timeuid.equals(other.timeuid))
			return false;
		return true;
	}

	public void setRoutingKey(String routingKey) {
		this.routingKey = routingKey;
	}

	public UUID getTimeuid() {
		return timeuid;
	}

	public void setTimeuid(UUID timeuid) {
		this.timeuid = timeuid;
	}

	private String routingKey;
	
	private UUID timeuid;
	
	
}
