package com.Rezar.dbSub.client.config;

import java.util.List;
import java.util.Map;

import lombok.Data;

/**
 * 客户端需要进行binlog订阅的库表信息
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 23, 2020 12:38:02 PM
 * @Desc 些年若许,不负芳华.
 *
 */
@Data
public class SingleDbInsClientConfigInfo {

	// 订阅的数据库实例名称
	private String dbIns;
	// db:table-->List(TableBinlogProcessor) ,单表的所有事件Processor
	private Map<TableInitKey, List<TableBinlogProcessor>> tableBinlogProcessorMap;

	public SingleDbInsClientConfigInfo(String dbIns,
			Map<TableInitKey, List<TableBinlogProcessor>> tableBinlogProcessorMap) {
		this.dbIns = dbIns;
		this.tableBinlogProcessorMap = tableBinlogProcessorMap;
	}

}
