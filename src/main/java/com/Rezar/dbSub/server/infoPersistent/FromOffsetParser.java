package com.Rezar.dbSub.server.infoPersistent;

public class FromOffsetParser {

	/**
	 * seqId的格式
	 * 
	 * (1)_(2)_(3)_(4)
	 * 
	 * 1部分:起始偏移的binlog文件的数字标记
	 * 
	 * 2部分:起始偏移的binlog事件的起始offset
	 * 
	 * 3部分:下一个事件的起始偏移的binlog文件的数字标记
	 * 
	 * 4部分:下一个事件的binlog事件的起始offset
	 * 
	 * @param seqId
	 * @return
	 */
	public static String parseNextSeqId(String seqId) {
		String[] split = seqId.split("_");
		return split[2] + "_" + split[3];
		// return String.valueOf(Long.parseLong(split[0]) + Long.parseLong(split[1]));
	}

	public static String parseNextSeqId(byte[] key) {
		return parseNextSeqId(new String(key));
	}

}
