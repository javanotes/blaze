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
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.core.type.classreading.CachingMetadataReaderFactory;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.reactivetechnologies.blaze.utils.JarFilesDeployer;
import com.reactivetechnologies.mq.Data;
import com.reactivetechnologies.mq.consume.Consumer;
import com.reactivetechnologies.mq.consume.QueueListener;
import com.reactivetechnologies.mq.consume.QueueListenerBuilder;
import com.reactivetechnologies.mq.container.QueueContainer;
import com.reactivetechnologies.mq.data.TextData;
@Component
public class DeploymentRunner implements CommandLineRunner {

	private static final Logger log = LoggerFactory.getLogger(DeploymentRunner.class);
	static final String DEFAULT_RESOURCE_PATTERN = "**/*.class";
	@Autowired
	private ApplicationContext context;
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
		newConsumer();
		loadDataClass();
	}
	private void newConsumer() throws InstantiationException, IllegalAccessException, ClassNotFoundException
	{
		classImpl = deployer.classForName(className).newInstance();
	}
	private void loadDataClass() throws ClassNotFoundException 
	{
		dataType = deployer.classForName(dataName);
	}
	private void register()
	{
		container.register(createListener());
		log.info("Registered queue listener ");
	}
	private void deploy()
	{
		try 
		{
			deployer.deployLibs(deployDir);
			loadClasses();
			register();
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
		
		if(StringUtils.hasText(deployDir))
		{
			log.info("Checking for deployments at- "+deployDir);
			deploy();
		}
		else
		{
			log.warn("No deployment directory specified");
			Object consumer = scanForConsumerClass();
			if(consumer == null)
			{
				consumer = performFullScan();
			}
						
			if(consumer != null)
			{
				classImpl = consumer;
				if (StringUtils.hasText(dataName)) {
					try {
						loadDataClass();
					} catch (ClassNotFoundException e) {
						throw new BeanCreationException("'Data' type not loaded", e);
					} 
				}
				else{
					dataType = TextData.class;
				}
				register();
			}
			else
			{
				log.error("** No consumer found for deployment. Container will shut down **");
			}
		}
	}
	private static Class<?> classForName(String className)
	{
		try {
			return ClassUtils.forName(className, Thread.currentThread().getContextClassLoader());
		} catch (ClassNotFoundException | LinkageError e) {
			return null;
		}
	}
	private Object getInstance(Class<?> clazz)
	{
		Object instance = null;
		try {
			instance = context.getBean(clazz);
		} catch (BeansException e) {
			log.info("Instance not found as a Spring bean. Trying to instantiate. Error => "+e.getMessage());
			log.debug("", e);
			try {
				instance = clazz.newInstance();
			} catch (InstantiationException | IllegalAccessException e1) {
				log.info("Unable to instantiate class. Error => "+e1.getMessage());
				log.debug("", e1);
			}
		}
		
		return instance;
	}
	
	private Object performFullScan() 
	{
		log.info("Performing a full classpath scan to find a first matching consumer class. This the final fallback..");
		PathMatchingResourcePatternResolver resourceResolver = new PathMatchingResourcePatternResolver();
		MetadataReaderFactory metadataReaderFactory = new CachingMetadataReaderFactory(resourceResolver);
		try {
			Resource[] resources = resourceResolver.getResources(ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX+DEFAULT_RESOURCE_PATTERN);
			MetadataReader reader;
			String[] ifaces;
			for(Resource res : resources)
			{
				reader = metadataReaderFactory.getMetadataReader(res);
				ifaces = reader.getClassMetadata().getInterfaceNames();
				
				if(ifaces.length > 0)
				{
					Arrays.sort(ifaces);
					if(Arrays.binarySearch(ifaces, Consumer.class.getName()) != -1)
					{
						try {
							return newInstance(reader);
						} catch (Throwable e) {
							log.error("Unable to instantiate consumer found in full classpath scan => "+ e.getMessage());
							log.debug("", e);
							return null;
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return null;
	}
	
	private Object newInstance(MetadataReader reader)
			throws ClassNotFoundException, LinkageError, InstantiationException, IllegalAccessException {
		return ClassUtils.forName(reader.getClassMetadata().getClassName(), Thread.currentThread().getContextClassLoader())
				.newInstance();
	}
	private Object scanByClassName()
	{
		log.info("Trying to load class");
		Class<?> clazz = classForName(className);
		if(clazz != null)
		{
			log.info("Class loaded. Checking for instance");
			return getInstance(clazz);
			
		}
		return null;
	}
	private Object scanFromContext()
	{
		log.info("Checking for a single matching consumer class from Spring context");
		try {
			return context.getBean(Consumer.class);
		} catch (BeansException e) {
			log.warn("No such class found in Spring context");
		}
		return null;
	}
	private Object scanForConsumerClass() 
	{
		Object consumer = null;
		if(StringUtils.hasText(className))
		{
			log.info("Consumer class name specified- "+className);
			consumer = scanByClassName();
		}
		else
		{
			consumer = scanFromContext();
		}
		return consumer;
		
	}

	@SuppressWarnings("unchecked")
	private <T extends Data> QueueListener<T> createListener() {
		log.info("Registering consumer of type ["+classImpl.getClass()+"], consuming Data of "+dataType.getName());
		return new QueueListenerBuilder()
				.dataType((Class<T>) dataType)
				.consumer((Consumer<T>) classImpl)
				.build();
	}

}
