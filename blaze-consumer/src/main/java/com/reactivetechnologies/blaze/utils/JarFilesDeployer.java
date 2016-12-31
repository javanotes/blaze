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
package com.reactivetechnologies.blaze.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.springframework.util.ResourceUtils;

import com.reactivetechnologies.mq.common.JarClassLoader;

public class JarFilesDeployer {

	private final JarClassLoader loader;
	public JarFilesDeployer() {
		this.loader = new JarClassLoader();
	}
	/**
	 * Tries on a best effort basis to load a file.
	 * @param path
	 * @return
	 * @throws FileNotFoundException
	 */
	public static File getAsResource(String path) throws FileNotFoundException
	{
		File f = null;
		try {
			f = ResourceUtils.getFile(ResourceUtils.CLASSPATH_URL_PREFIX+path);
		} catch (FileNotFoundException e) {
			try {
				f = ResourceUtils.getFile(ResourceUtils.FILE_URL_PREFIX+path);
			} catch (FileNotFoundException e1) {
				try {
					ResourceUtils.getFile(Thread.currentThread().getContextClassLoader().getResource(path));
				} catch (Exception e2) {
					FileNotFoundException fnf = new FileNotFoundException();
					fnf.initCause(e2);
					throw fnf;
				}
			}
		}
		return f;
	}
	/**
	 * Add jars given under the directory to class path.
	 * @param dir
	 * @throws IOException
	 */
	public void deployLibs(String dir) throws IOException
	{
		File f = getAsResource(dir);
		loader.addJars(f);
	}
	/**
	 * Load a given jar file to class path.
	 * @param dir
	 * @throws IOException
	 */
	public void deployLib(String path) throws IOException
	{
		File f = getAsResource(path);
		loader.addJar(f);
	}
	/**
	 * Class.forName() for externally deployed classes.
	 * @param name
	 * @return
	 * @throws ClassNotFoundException
	 */
	public Class<?> classForName(String name) throws ClassNotFoundException
	{
		return loader.loadClassForName(name);
	}
}
