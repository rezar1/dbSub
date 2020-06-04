package com.Rezar.dbSub.server.relay;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import com.Rezar.dbSub.base.dbInfo.SubTableInfoForServer;
import com.Rezar.dbSub.base.dbInfo.TableInfo;
import com.Rezar.dbSub.utils.GU;

import lombok.Getter;

public class WatchTableFilter {

	public static final WatchTableFilter FALSE_FILTER = new WatchTableFilter(null, null);

	@Getter
	private String key;
	private TableInfo tableInfo;
	private SubTableInfoForServer syncTableInfoConfig;
	private Set<String> excludeFieldNames = new HashSet<>(3);

	public WatchTableFilter(TableInfo tableInfo, SubTableInfoForServer syncTableInfoConfig) {
		if (tableInfo == null || syncTableInfoConfig == null) {
			return;
		}
		this.key = tableInfo.getDb() + "_" + tableInfo.getTableName();
		this.tableInfo = tableInfo;
		this.syncTableInfoConfig = syncTableInfoConfig;
		String filterTimeChange = this.syncTableInfoConfig.getFilterTimeChange();
		if (GU.notNullAndEmpty(filterTimeChange)) {
			this.excludeFieldNames.addAll(Arrays.asList(filterTimeChange.split(",")));
		}
	}

	public boolean filter(Serializable[] columnCur, Serializable[] columnBefore) {
		if (this == FALSE_FILTER) {
			return false;
		}
		if (columnCur != null && columnBefore != null && columnCur.length == columnBefore.length) {
			return tableInfo.compareIfSame(columnCur, columnBefore, excludeFieldNames);
		}
		return false;
	}

}
