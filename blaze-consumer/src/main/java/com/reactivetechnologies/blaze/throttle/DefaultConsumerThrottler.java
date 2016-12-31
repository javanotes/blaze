/**
 * Copyright 2016 esutdal

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.reactivetechnologies.blaze.throttle;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.chain.Command;
import org.apache.commons.chain.impl.ChainBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.reactivetechnologies.blaze.handlers.ThrottlingCommandHandlerFactory;
import com.reactivetechnologies.mq.common.BlazeInternalError;
class DefaultConsumerThrottler extends ChainBase implements ConsumerThrottler{

	private long throttlerPeriod;
	private boolean enabled;
	
	public long getThrottlerPeriod() {
		return throttlerPeriod;
	}
	public void setThrottlerPeriod(long throttlerPeriod) {
		this.throttlerPeriod = throttlerPeriod;
	}
	public boolean isEnabled() {
		return enabled;
	}
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}
	private ScheduledExecutorService timer;
	private AtomicInteger counter;
	private static final Logger log = LoggerFactory.getLogger(DefaultConsumerThrottler.class);
	
	private class MessageThrottlerTask implements Runnable
	{
		
		@Override
		public void run() {
			while(!counter.compareAndSet(getCount(), 0));
			log.debug("Set");
		}
		
	}
	/**
	 * 
	 */
	public DefaultConsumerThrottler() {
		super();
	}
	//set by the factory bean
	ThrottlingCommandHandlerFactory otherCommands;
	/**
	 * Provision to add more commands. The first command, which is mandatory and system defined, will set the current count in context.
	 * So subsequent commands can refer to it if needed.
	 */
	protected List<? extends Command> loadCommands()
	{
		return otherCommands.getCommands();
	}
	@PostConstruct
	public void init()
	{
		if (enabled) 
		{
			addCommand(new ThrottleCommand());
			List<? extends Command> customCommands = loadCommands();
			if(customCommands != null)
			{
				for(Command cmd : customCommands)
					addCommand(cmd);
			}
			
			counter = new AtomicInteger();
			timer = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {

				@Override
				public Thread newThread(Runnable arg0) {
					Thread t = new Thread(arg0, "Throttler-timer-thread");
					t.setDaemon(true);
					return t;
				}
			});
			timer.scheduleWithFixedDelay(new MessageThrottlerTask(), 0, throttlerPeriod, TimeUnit.MILLISECONDS);
			
			log.info("Throttling enabled with period of "+throttlerPeriod+" millis");
		}
	}
	@PreDestroy
	public void destroy()
	{
		if (timer != null) {
			timer.shutdown();
			try {
				timer.awaitTermination(1, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				
			}
		}
	}
	public int getCount()
	{
		return counter.addAndGet(0);
	}
	/* (non-Javadoc)
	 * @see com.blaze.mq.redis.throttle.IConsumerThrottler#incrementCount()
	 */
	@Override
	public void incrementCount()
	{
		if (enabled) {
			counter.incrementAndGet();
		}
	}
	/* (non-Javadoc)
	 * @see com.blaze.mq.redis.throttle.IConsumerThrottler#allowMessageConsume(int)
	 */
	@Override
	public boolean allowMessageConsume(int throttleTps) 
	{
		if(!enabled)
			return true;
		MTContext ctx = new MTContext(throttleTps, this);
		try {
			execute(ctx);
		} catch (Exception e) {
			throw new BlazeInternalError("Exception in message throtller", e);
		}
		return !ctx.isThrottle();
		
	}

}
