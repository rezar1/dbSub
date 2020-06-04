package com.Rezar.dbSub.utils;

import java.util.regex.Pattern;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年12月6日 下午5:57:55
 * @Desc 些年若许,不负芳华.
 *
 */
public class BinlogUtils {

	public static final Object SERVER_CLOSE_EVENT = new Object();

	public static String dbAndTable(String db, String table) {
		return db + ":" + table;
	}

	public static boolean checkOffset(String offset) {
		return Pattern.compile("(\\d+_?){3}").matcher(offset).matches();
	}

}
