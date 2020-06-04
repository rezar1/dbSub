package com.Rezar.dbSub.utils;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月26日 下午6:48:45
 * @Desc 些年若许,不负芳华.
 *
 */
public class StrNameConvert {

	public static void main(String[] args) {
		System.out.println(underlineToHump("user_info"));
	}

	/***
	 * 下划线命名转为驼峰命名
	 * 
	 * @param para
	 *            下划线命名的字符串
	 */

	public static String underlineToHump(String para) {
		StringBuilder result = new StringBuilder();
		String a[] = para.split("_");
		for (String s : a) {
			if (result.length() == 0) {
				result.append(s.toLowerCase());
			} else {
				result.append(s.substring(0, 1).toUpperCase());
				result.append(s.substring(1).toLowerCase());
			}
		}
		return result.toString();
	}

	/***
	 * 驼峰命名转为下划线命名
	 * 
	 * @param para
	 *            驼峰命名的字符串
	 */

	public String humpToUnderline(String para) {
		StringBuilder sb = new StringBuilder(para);
		int temp = 0;// 定位
		for (int i = 0; i < para.length(); i++) {
			if (Character.isUpperCase(para.charAt(i))) {
				sb.insert(i + temp, "_");
				temp += 1;
			}
		}
		return sb.toString().toUpperCase();
	}

}
