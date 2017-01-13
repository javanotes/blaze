/* ============================================================================
*
* FILE: DirectMem.java
*
The MIT License (MIT)

Copyright (c) 2016 Sutanu Dalui

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*
* ============================================================================
*/
package com.reactivetechnologies.mq.disk;

import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;

import org.springframework.util.ReflectionUtils;

class DirectMem {

	/**
	 * 
	 * @param bb
	 * @return
	 */
	public static boolean unmap(MappedByteBuffer bb) {
		/*
		 * From sun.nio.ch.FileChannelImpl private static void
		 * unmap(MappedByteBuffer bb) {
		 * 
		 * Cleaner cl = ((DirectBuffer)bb).cleaner(); if (cl != null)
		 * cl.clean(); }
		 */
		try {
			Method cleaner_method = ReflectionUtils.findMethod(bb.getClass(), "cleaner");
			if (cleaner_method != null) {
				cleaner_method.setAccessible(true);
				Object cleaner = cleaner_method.invoke(bb);
				if (cleaner != null) {
					Method clean_method = ReflectionUtils.findMethod(cleaner.getClass(), "clean");
					clean_method.setAccessible(true);
					clean_method.invoke(cleaner);
					return true;
				}
			}

		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return false;
	}

}
