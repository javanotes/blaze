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
package com.reactivetechnologies.blaze.struct;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.UUID;

import org.springframework.data.redis.serializer.RedisSerializer;
import org.springframework.data.redis.serializer.SerializationException;

public class QRecordSerializer implements RedisSerializer<QRecord> {

	@Override
	public byte[] serialize(QRecord t) throws SerializationException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try 
		{
			if (t != null) {
				DataOutputStream d = new DataOutputStream(out);
				d.writeLong(t.getT0TS() != null ? t.getT0TS().getTime() : -1);
				d.writeLong(t.getTnTS() != null ? t.getTnTS().getTime() : -1);
				d.writeLong(t.getExpiryMillis());
				UUID u = t.getKey().getTimeuid();
				d.writeLong(u.getMostSignificantBits());
				d.writeLong(u.getLeastSignificantBits());
				d.writeShort(t.getRedeliveryCount());
				d.writeUTF(t.getKey().getExchange());
				d.writeUTF(t.getKey().getRoutingKey());
				d.writeUTF(t.getCorrId());
				d.writeUTF(t.getReplyTo());
				d.writeBoolean(t.isRedelivered());
				d.write(t.getPayload().array());
			}
			
		} 
		catch (Exception e) 
		{
			throw new SerializationException("I/O exception on serialize", e);
		}
		return out.toByteArray();
	}

	@Override
	public QRecord deserialize(byte[] bytes) throws SerializationException {
		try 
		{
			if (bytes != null) 
			{
				QRecord qr = new QRecord();
				DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
				long time = in.readLong();
				qr.setT0TS(time == -1 ? null : new Date(time));
				time = in.readLong();
				qr.setTnTS(time == -1 ? null : new Date(time));
				qr.setExpiryMillis(in.readLong());
				UUID timeuid = new UUID(in.readLong(), in.readLong());
				QKey qk = new QKey();
				qk.setTimeuid(timeuid);
				qr.setRedeliveryCount(in.readShort());
				qk.setExchange(in.readUTF());
				qk.setRoutingKey(in.readUTF());
				qr.setKey(qk);
				qr.setCorrId(in.readUTF());
				qr.setReplyTo(in.readUTF());
				qr.setRedelivered(in.readBoolean());
				int len = in.available();
				byte[] b = new byte[len];
				in.readFully(b);
				qr.setPayload(ByteBuffer.wrap(b));
				
				return qr;
			}
			
		} catch (Exception e) {
			throw new SerializationException("I/O exception on deserialize", e);
		}
		return null;
		
	}

}
