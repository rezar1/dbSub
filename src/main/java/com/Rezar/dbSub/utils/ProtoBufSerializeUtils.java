package com.Rezar.dbSub.utils;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.protostuff.Input;
import io.protostuff.LinkedBuffer;
import io.protostuff.Output;
import io.protostuff.Pipe;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.WireFormat.FieldType;
import io.protostuff.runtime.DefaultIdStrategy;
import io.protostuff.runtime.Delegate;
import io.protostuff.runtime.RuntimeEnv;
import io.protostuff.runtime.RuntimeSchema;

public class ProtoBufSerializeUtils {

	/** 时间戳转换Delegate，解决时间戳转换后错误问题 @author jiujie 2016年7月20日 下午1:52:25 */
	private final static Delegate<Timestamp> TIMESTAMP_DELEGATE = new TimestampDelegate();

	private final static DefaultIdStrategy idStrategy = ((DefaultIdStrategy) RuntimeEnv.ID_STRATEGY);

	static {
		idStrategy.registerDelegate(TIMESTAMP_DELEGATE);
	}

	/**
	 * /** 缓存Schema
	 */
	private static Map<Class<?>, Schema<?>> schemaCache = new ConcurrentHashMap<Class<?>, Schema<?>>();

	@SuppressWarnings("unchecked")
	public static <T> byte[] serialize(T obj) {
		if (obj == null) {
			throw new NullPointerException();
		}
		Class<T> clazz = (Class<T>) obj.getClass();
		Schema<T> schema = getSchema(clazz);
		LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
		try {
			return ProtostuffIOUtil.toByteArray(obj, schema, buffer);
		} finally {
			buffer.clear();
		}
	}

	public static <T> T deserialize(byte[] data, Class<T> clazz) {
		Schema<T> schema = getSchema(clazz);
		T obj = schema.newMessage();
		ProtostuffIOUtil.mergeFrom(data, obj, schema);
		return obj;
	}

	@SuppressWarnings("unchecked")
	private static <T> Schema<T> getSchema(Class<T> clazz) {
		Schema<T> schema = (Schema<T>) schemaCache.get(clazz);
		if (schema == null) {
			// 这个schema通过RuntimeSchema进行懒创建并缓存
			// 所以可以一直调用RuntimeSchema.getSchema(),这个方法是线程安全的
			schema = RuntimeSchema.getSchema(clazz, idStrategy);
			if (schema != null) {
				schemaCache.put(clazz, schema);
			}
		}

		return schema;
	}

	public static class TimestampDelegate implements Delegate<Timestamp> {

		public FieldType getFieldType() {
			return FieldType.FIXED64;
		}

		public Class<?> typeClass() {
			return Timestamp.class;
		}

		public Timestamp readFrom(Input input) throws IOException {
			return new Timestamp(input.readFixed64());
		}

		public void writeTo(Output output, int number, Timestamp value, boolean repeated) throws IOException {
			output.writeFixed64(number, value.getTime(), repeated);
		}

		public void transfer(Pipe pipe, Input input, Output output, int number, boolean repeated) throws IOException {
			output.writeFixed64(number, input.readFixed64(), repeated);
		}

	}

}