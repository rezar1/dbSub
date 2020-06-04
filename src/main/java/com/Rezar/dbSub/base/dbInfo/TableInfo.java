package com.Rezar.dbSub.base.dbInfo;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import com.Rezar.dbSub.server.dbInfo.TableColumnList;
import com.Rezar.dbSub.server.dbInfo.TableColumnList.ColumnDef;
import com.Rezar.dbSub.utils.SeriAndDeser;

import lombok.Data;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月24日 下午7:13:37
 * @Desc 些年若许,不负芳华.
 *
 */
@Data
public class TableInfo {

	private String db;
	private String tableName;
	private TableColumnList columns = new TableColumnList();
	private String charset;

	public TableInfo(String dbName, String tableName, String charset) {
		this.db = dbName;
		this.tableName = tableName;
		this.charset = charset;
	}

	public void addColumnDef(ColumnDef columnDef) {
		this.columns.addColumnDef(columnDef);
	}

	public HashMap<String, Object> parseToValue(Serializable[] columnDatas) {
		// 看下怎么复用这个map TODO
		HashMap<String, Object> dataMap = new HashMap<>();
		List<ColumnDef> tableColumnes = columns.getColumns();
		for (int index = 0; index < columnDatas.length; index++) {
			ColumnDef columnDef = tableColumnes.get(index);
			Object value = transFor(columnDef, columnDatas[index]);
			dataMap.put(columnDef.getName(), value);
		}
		return dataMap;
	}

	/**
	 * 进入该方法前应该确保
	 * columnDatasCur!=null&&columnDatasBefore!=null&&columnDatasCur.len=columnDatasBefore.len
	 * 
	 * @param columnDatasCur
	 * @param columnDatasBefore
	 * @param excludeFields
	 * @return
	 */
	public boolean compareIfSame(Serializable[] columnDatasCur, Serializable[] columnDatasBefore,
			Set<String> excludeFields) {
		boolean ifSame = true;
		List<ColumnDef> tableColumnes = columns.getColumns();
		for (int index = 0; index < tableColumnes.size(); index++) {
			ColumnDef columnDef = tableColumnes.get(index);
			String columnName = columnDef.getName();
			if (excludeFields.contains(columnName)) {
				continue;
			}
			Object curOfIndex = transFor(tableColumnes.get(index), columnDatasCur[index]);
			Object befOfIndex = transFor(tableColumnes.get(index), columnDatasBefore[index]);
			int nullCount = (curOfIndex == null ? 1 : 0) + (befOfIndex == null ? 1 : 0);
			if (nullCount != 2 && ((nullCount == 0 && !curOfIndex.equals(befOfIndex)) || (nullCount == 1))) {
				ifSame = false;
				break;
			}
		}
		return ifSame;
	}

	/**
	 * @param columnDef
	 * @param value
	 * @return
	 */
	private Object transFor(ColumnDef columnDef, Serializable value) {
		if (value == null) {
			return null;
		}
		if (value instanceof byte[] && columnDef.getType() == SeriAndDeser.STRING) {
			try {
				value = new String((byte[]) value, "utf-8");
			} catch (UnsupportedEncodingException e) {
				value = new String((byte[]) value);
			}
		}
		return value;
	}

}
