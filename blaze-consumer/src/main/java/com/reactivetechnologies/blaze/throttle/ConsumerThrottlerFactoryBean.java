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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.PreDestroy;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.reactivetechnologies.blaze.handlers.ThrottlingCommandHandlerFactory;
@Component
public class ConsumerThrottlerFactoryBean implements FactoryBean<DefaultConsumerThrottler> {

	public ConsumerThrottlerFactoryBean() {
	}

	private List<DefaultConsumerThrottler> register = Collections.synchronizedList(new ArrayList<>());
	@Override
	public DefaultConsumerThrottler getObject() throws Exception {
		return new DefaultConsumerThrottler();
	}
	@PreDestroy
	private void destroy()
	{
		for(DefaultConsumerThrottler each : register)
			each.destroy();
	}
	@Autowired
	private ThrottlingCommandHandlerFactory otherCommands;
	/**
	 * 
	 * @param throttlerPeriod
	 * @param enabled
	 * @return
	 * @throws Exception
	 */
	public ConsumerThrottler getObject(long throttlerPeriod, boolean enabled) throws Exception {
		DefaultConsumerThrottler cthrot =  new DefaultConsumerThrottler();
		cthrot.setEnabled(enabled);
		cthrot.setThrottlerPeriod(throttlerPeriod);
		cthrot.otherCommands = this.otherCommands;
		cthrot.init();
		register.add(cthrot);
		return cthrot;
	}

	@Override
	public Class<?> getObjectType() {
		return DefaultConsumerThrottler.class;
	}

	@Override
	public boolean isSingleton() {
		return false;
	}

}
