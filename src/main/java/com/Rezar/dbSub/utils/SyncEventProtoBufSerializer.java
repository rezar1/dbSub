package com.Rezar.dbSub.utils;

import java.sql.Timestamp;

import com.Rezar.dbSub.base.event.SyncEvent;
import com.Rezar.dbSub.utils.ProtoBufSerializeUtils.TimestampDelegate;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.DefaultIdStrategy;
import io.protostuff.runtime.Delegate;
import io.protostuff.runtime.RuntimeEnv;
import io.protostuff.runtime.RuntimeSchema;

/**
 * 线程不安全,请勿多线程访问
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 27, 2020 11:37:58 PM
 * @Desc 些年若许,不负芳华.
 *
 */
public class SyncEventProtoBufSerializer {

	public static SyncEventProtoBufSerializer instance() {
		return new SyncEventProtoBufSerializer();
	}

	/** 时间戳转换Delegate，解决时间戳转换后错误问题 */
	private final static Delegate<Timestamp> TIMESTAMP_DELEGATE = new TimestampDelegate();
	private final static DefaultIdStrategy idStrategy = ((DefaultIdStrategy) RuntimeEnv.ID_STRATEGY);

	static {
		idStrategy.registerDelegate(TIMESTAMP_DELEGATE);
	}

	private final Schema<SyncEvent> schema = RuntimeSchema.getSchema(SyncEvent.class, idStrategy);
	private final LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);

	public byte[] serialize(SyncEvent obj) {
		if (obj == null) {
			throw new NullPointerException();
		}
		try {
			return ProtostuffIOUtil.toByteArray(obj, schema, buffer);
		} finally {
			buffer.clear();
		}
	}

	public SyncEvent deserializeFull(byte[] data) {
		SyncEvent deserializeJustInitSeqId = schema.newMessage();
		ProtostuffIOUtil.mergeFrom(data, deserializeJustInitSeqId, schema);
		return deserializeJustInitSeqId;
	}

}