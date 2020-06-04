package com.Rezar.dbSub.utils;

import org.apache.commons.lang3.StringUtils;

public class PatternUtils {

	/**
	 * binlog文件好像是可配置的,TODO
	 * 
	 * @param fileName
	 * @return
	 */
	public static boolean isBinlogFile(String fileName) {
		return true;
	}

	public static boolean isBinlogOffset(String offset) {
		return StringUtils.isNumeric(offset);
	}

}
