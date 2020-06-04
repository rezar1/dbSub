package com.Rezar.dbSub.utils;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月22日 下午7:03:46
 * @Desc 些年若许,不负芳华.
 *
 */
public class SubInfoMark {

	private static final String format = "Sub_%s_%s_%s";

	public static String getSubInfoMark(String dbInstanceName, String subDB, String subTable) {
		return String.format(format, dbInstanceName, subDB, subTable);
	}

}
