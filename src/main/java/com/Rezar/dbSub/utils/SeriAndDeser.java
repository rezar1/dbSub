package com.Rezar.dbSub.utils;

import java.nio.ByteBuffer;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2017年9月29日
 * @Desc this guy is to lazy , noting left.
 *
 */
public interface SeriAndDeser<V> {

	public static final byte STRING = 1;
	public static final byte INTEGER = 2;
	public static final byte DOUBLE = 3;
	public static final byte LONG = 4;
	public static final byte BOOLEAN = 5;
	public static final byte FLOAT = 6;
	public static final byte BYTE = 7;
	public static final byte DATE = 8;
	public static final byte TIMESTAMP = 9;
	public static final byte NULL = 10;
	public static final byte CHAR = 11;
	public static final byte OBJ = 12;
	public static final byte DECIMAL = 13;
	public static final byte DATETIME = 14;
	public static final byte TIME = 15;

	public void serialize(V obj, ByteBuffer buff);

	public V deserialize(ByteBuffer buf);

}
