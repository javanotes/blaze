package com.reactivetechnologies.blaze;

import java.util.Collections;
import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.reactivetechnologies.blaze.core.DefaultConsumerRecoveryHandler;
import com.reactivetechnologies.blaze.core.DefaultDeadLetterHandler;
import com.reactivetechnologies.blaze.handlers.ConsumerRecoveryHandler;
import com.reactivetechnologies.blaze.handlers.DeadLetterHandler;
import com.reactivetechnologies.blaze.handlers.ThrottlingCommandHandler;
import com.reactivetechnologies.blaze.handlers.ThrottlingCommandHandlerFactory;
import com.reactivetechnologies.blaze.ops.BlazeRedisTemplate;
import com.reactivetechnologies.blaze.ops.RedisDataAccessor;
import com.reactivetechnologies.blaze.utils.JarFilesDeployer;

/*
 * -------------------------------------------------------
 *  REDIS Config.
 *  Refer to http://oldblog.antirez.com/post/redis-persistence-demystified.html for an understanding on the persistence in Redis.
 *  This can significantly impact the performance.
 *  
 *  Minimal changes advised, over the default Redis config:
 *  	appendonly yes
 *  	appendfsync everysec
 *  
 *  For a comprehensive understanding, please study Redis tuning in general.
 * ------------------------------------------------------- 
 */
@Configuration
public class Config {
	@Bean
	BlazeRedisTemplate template(RedisConnectionFactory connectionFactory)
	{
		BlazeRedisTemplate rt = new BlazeRedisTemplate();
		rt.setConnectionFactory(connectionFactory);
		rt.setSerializers();
		return rt;
	}
	
	@Bean
	StringRedisTemplate stringTemplate(RedisConnectionFactory connectionFactory)
	{
		return new StringRedisTemplate(connectionFactory);
	}
	
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
	ConsumerRecoveryHandler recoveryHandler(RedisDataAccessor redisOps)
	{
		return new DefaultConsumerRecoveryHandler();
	}
				
}
