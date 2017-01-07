package com.reactivetechnologies.blaze;

import java.util.Collections;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.reactivetechnologies.blaze.core.DefaultConsumerRecoveryHandler;
import com.reactivetechnologies.blaze.core.DefaultDeadLetterHandler;
import com.reactivetechnologies.blaze.handlers.ConsumerRecoveryHandler;
import com.reactivetechnologies.blaze.handlers.DeadLetterHandler;
import com.reactivetechnologies.blaze.handlers.ThrottlingCommandHandler;
import com.reactivetechnologies.blaze.handlers.ThrottlingCommandHandlerFactory;
import com.reactivetechnologies.blaze.utils.JarFilesDeployer;


@Configuration
@Import(RedisConfig.class)
public class Config {
		
	@Bean
	JarFilesDeployer deployer()
	{
		return new JarFilesDeployer();
	}
	
	//-----------------------------
	//Declare various handlers here.
	//These classes will be the adaptation
	//points for customization.
	//-----------------------------
	
	//NOOP as of now
	//Probably can be left as-is
	//@see DefaultConsumerThrottler
	@Bean
	ThrottlingCommandHandlerFactory throttlingHandlerFactory()
	{
		return new ThrottlingCommandHandlerFactory() {
			
			@Override
			public List<ThrottlingCommandHandler> getCommands() {
				return Collections.emptyList();
			}
		};
	}
	
	@Bean
	DeadLetterHandler deadLetterHandler()
	{
		return new DefaultDeadLetterHandler();
	}
	@Bean
	ConsumerRecoveryHandler recoveryHandler()
	{
		return new DefaultConsumerRecoveryHandler();
	}
				
}
