/**
 * Copyright 2017 esutdal

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
package com.reactivetechnologies.blaze.ops;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.reactivetechnologies.blaze.struct.QRecord;
import com.reactivetechnologies.blaze.struct.QRecordSerializer;
import com.reactivetechnologies.mq.DataSerializable;

public class QRecordExternalizable implements DataSerializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1122510215629918341L;
	@Override
	public String toString() {
		return "[preparedKey=" + preparedKey + ", record=" + record + "]";
	}

	public QRecord getRecord() {
		return record;
	}

	public void setRecord(QRecord record) {
		this.record = record;
	}
	private String preparedKey;
	private QRecord record;
	private static QRecordSerializer serializer = new QRecordSerializer();
	public QRecordExternalizable(QRecord record) {
		super();
		this.record = record;
	}
	public QRecordExternalizable() {
	}
	
	private void readExternal(DataInput in) throws IOException {
		int i = in.readInt();
		byte[] b = new byte[i];
		in.readFully(b);
		record = serializer.deserialize(b);
		setPreparedKey(in.readUTF());
	}

	
	private void writeExternal(DataOutput out) throws IOException {
		byte[] b = serializer.serialize(record);
		out.writeInt(b.length);
		//System.out.println("write len: "+b.length);
		out.write(b);
		//System.out.println("write bytes: "+b.length);
		out.writeUTF(getPreparedKey());
		//System.out.println("write utf: "+getPreparedKey());
	}

	public String getPreparedKey() {
		return preparedKey;
	}

	public void setPreparedKey(String preparedKey) {
		this.preparedKey = preparedKey;
	}

	@Override
	public void writeData(DataOutput out) throws IOException {
		writeExternal(out);
	}

	@Override
	public void readData(DataInput in) throws IOException {
		readExternal(in);
	}

}
