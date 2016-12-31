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
package com.reactivetech.messaging;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.ResourceUtils;

import com.reactivetechnologies.blaze.BlazeConsumer;
import com.reactivetechnologies.mq.common.JarClassLoader;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = BlazeConsumer.class)
public class JarClassloaderTest {

	private static class ClassLoadedByParent
	{
		
	}
	@Test
	public void testLoadClassFromExternalJar() throws IOException, Exception
	{
		File f = ResourceUtils.getFile(ResourceUtils.CLASSPATH_URL_PREFIX+"json-simple-1.1.1.jar");
		Assert.assertNotNull(f);
		try (JarClassLoader loader = new JarClassLoader()) {
			loader.addJar(f);
			Object jsonObject = loader.loadClassForName("org.json.simple.JSONObject").newInstance();
			Assert.assertNotNull(jsonObject);
			
			ClassLoadedByParent obj = new ClassLoadedByParent();
			
			Method put = ReflectionUtils.findMethod(jsonObject.getClass(), "put", Object.class, Object.class);
			Assert.assertNotNull("method not found", put);
			
			put.invoke(jsonObject, "key", obj);
		}
		
	}
	
	@Test
	public void testLoadClassFromExternalJarWithDependencies() throws IOException, Exception
	{
		File f = ResourceUtils.getFile(ResourceUtils.CLASSPATH_URL_PREFIX+"json-simple-1.1.1.jar");
		Assert.assertNotNull(f);
		try (JarClassLoader loader = new JarClassLoader()) {
			loader.addJar(f);
			
			Object jsonObject = loader.loadClassForName("org.json.simple.JSONObject").newInstance();
			Assert.assertNotNull(jsonObject);
			
			f = ResourceUtils.getFile(ResourceUtils.CLASSPATH_URL_PREFIX+"jars/");
			Assert.assertNotNull(f);
			loader.addJars(f);
			
			Method put = ReflectionUtils.findMethod(jsonObject.getClass(), "put", Object.class, Object.class);
			Assert.assertNotNull("method:put not found in "+jsonObject.getClass(), put);
			
			Object dbConnector = loader.loadClassForName("server.DBConnector").newInstance();
			Assert.assertNotNull(dbConnector);
			
			Method checkJdbcDriverIsPresent = ReflectionUtils.findMethod(dbConnector.getClass(), "checkJdbcDriverIsPresent");
			Assert.assertNotNull("method:checkJdbcDriverIsPresent not found in "+dbConnector.getClass(), checkJdbcDriverIsPresent);
			
			checkJdbcDriverIsPresent.invoke(dbConnector);
			
			put.invoke(jsonObject, "key", dbConnector);
		}
		
	}

}
