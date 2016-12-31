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
package com.reactivetechnologies.blaze.cfg;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.reactivetechnologies.blaze.utils.JarFilesDeployer;
import com.reactivetechnologies.mq.Data;
import com.reactivetechnologies.mq.consume.Consumer;
import com.reactivetechnologies.mq.consume.QueueListener;
import com.reactivetechnologies.mq.consume.QueueListenerBuilder;
import com.reactivetechnologies.mq.container.QueueContainer;
@Component
public class DeploymentRunner implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(DeploymentRunner.class);

	@Value("${consumer.deploy.dir:}")
	private String deployDir;
	@Value("${consumer.class.impl:}")
	private String className;
	@Value("${consumer.data.impl:}")
	private String dataName;
	
	@Autowired
	private JarFilesDeployer deployer;
	
	@Autowired
	private QueueContainer container;
	private Class<?> dataType;
	private Object classImpl;
	private void loadClasses() throws ClassNotFoundException, InstantiationException, IllegalAccessException
	{
		classImpl = deployer.classForName(className).newInstance();
		dataType = deployer.classForName(dataName);
	}
	
	private void deploy()
	{
		try 
		{
			deployer.deployLibs(deployDir);
			loadClasses();
			container.register(createListener());
			
			log.info("Deployment complete for consumer");
		} 
		catch (IOException e) {
			log.error("Unable to deploy jar. See nested exception", e);
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
			log.error("Unable to find impl classes in deployment", e);
		}
	}
	@Override
	public void run(String... args)  {
		log.info("Checking for deployments..");
		if(StringUtils.hasText(deployDir))
		{
			deploy();
		}
		else
		{
			log.warn("* No deployables found *");
		}
	}
	
	@SuppressWarnings("unchecked")
	private <T extends Data> QueueListener<T> createListener() {
		return new QueueListenerBuilder()
				.dataType((Class<T>) dataType)
				.consumer((Consumer<T>) classImpl)
				.build();
	}

}
