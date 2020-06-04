package com.Rezar.dbSub.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {

	/**
	 * 
	 * 24小时制
	 * 
	 * @param beginDate 必须是两位 00-24
	 * @param endDate   必须是两位 00-24
	 * @return
	 */
	public static Date randomNextDayDateBetHour(String beginDate, String endDate) {
		String curDay = formatyyyyMMdd(org.apache.commons.lang3.time.DateUtils.addDays(new Date(), 1));
		return randomDate(curDay + " " + beginDate, curDay + " " + endDate, "yyyy-MM-dd HH");
	}

	/**
	 * 
	 * 24小时制
	 * 
	 * @param beginDate 必须是两位 00-24
	 * @param endDate   必须是两位 00-24
	 * @return
	 */
	public static Date randomTodayDateBetHour(String beginDate, String endDate) {
		String curDay = formatyyyyMMdd(new Date());
		return randomDate(curDay + " " + beginDate, curDay + " " + endDate, "yyyy-MM-dd HH");
	}

	public static String formatyyyyMMdd(Date date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
		return format.format(date);
	}
	public static String formatyyyyMMddHHmmss(Date date) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		return format.format(date);
	}

	/**
	 * 生成随机时间
	 * 
	 * @param beginDate
	 * @param endDate
	 * @return
	 */
	public static Date randomDate(String beginDate, String endDate, String formatStr) {
		try {
			SimpleDateFormat format = new SimpleDateFormat(formatStr);
			Date start = format.parse(beginDate);// 构造开始日期
			Date end = format.parse(endDate);// 构造结束日期
			if (start.getTime() >= end.getTime()) {
				return null;
			}
			long date = random(start.getTime(), end.getTime());
			return new Date(date);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public static long random(long begin, long end) {
		long rtn = begin + (long) (Math.random() * (end - begin));
		// 如果返回的是开始时间和结束时间，则递归调用本函数查找随机值
		if (rtn == begin || rtn == end) {
			return random(begin, end);
		}
		return rtn;
	}

	public static void main(String[] args) {
//		Date randomDate = randomNextDayDateBetHour("01", "02");
		Date randomDate = randomFutureDate(10, 30);
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String result = format.format(randomDate);
		System.out.println(result);
	}

	public static Date randomFutureDate(int minuteAtLeast, int minuteMax) {
		Date curDate = new Date();
		return new Date(random(org.apache.commons.lang3.time.DateUtils.addMinutes(curDate, minuteAtLeast).getTime(),
				org.apache.commons.lang3.time.DateUtils.addMinutes(curDate, minuteMax).getTime()));
	}

}
