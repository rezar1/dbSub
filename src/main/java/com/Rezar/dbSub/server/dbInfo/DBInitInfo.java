package com.Rezar.dbSub.server.dbInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.Rezar.dbSub.base.dbInfo.SubTableInfoForServer;
import com.Rezar.dbSub.base.dbInfo.TableInfo;

import lombok.Data;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月24日 下午5:47:09
 * @Desc 些年若许,不负芳华.
 *
 */
@Data
public class DBInitInfo {

	private String db;
	private List<SubTableInfoForServer> subTableInfo = new ArrayList<>();
	private Map<String, TableInfo> tableInfoMapper = new HashMap<>();
	private Map<String, SubTableInfoForServer> subTableInfoMapper;

	/**
	 * @param tableName
	 * @return
	 */
	public TableInfo findTableInfo(String tableName) {
		return tableInfoMapper.get(tableName);
	}

	public void initSubTableMapper() {
		this.subTableInfoMapper = this.subTableInfo.stream()
				.collect(Collectors.toMap(SubTableInfoForServer::getSubTable, val -> val));
	}

	public boolean containeSubTable(String table) {
		return this.subTableInfoMapper.containsKey(table);
	}

	public Stream<TableInfo> streamSubTableToTableInfo() {
		return this.subTableInfo.stream().map(sub -> tableInfoMapper.get(sub.getSubTable()));
	}

}
