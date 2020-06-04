package com.Rezar.dbSub.server.dbInfo;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.Rezar.dbSub.base.dbInfo.SubTableInfoForServer;
import com.Rezar.dbSub.base.dbInfo.TableInfo;
import com.Rezar.dbSub.base.enums.ChangeType;
import com.Rezar.dbSub.server.interfaces.DbInsPositionRecorder;
import com.Rezar.dbSub.server.relay.WatchTableFilter;
import com.github.shyiko.mysql.binlog.event.EventType;

import lombok.Data;
import snaq.db.ConnectionPool;

/**
 * 单个db实例的信息,包含<br/>
 * 1) db连接池 <br/>
 * 2) binlog读取位移记录器 <br/>
 * 3)
 * 
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 12, 2020 2:51:50 PM
 * @Desc 些年若许,不负芳华.
 *
 */
@Data
public class DbInstanceInfo {

	private DatabaseInitInfo databaseInitInfo;
	private ConnectionPool connectionPool;
	private DbInsPositionRecorder dbPositionRecorder;
	private DBInfoRecovery recovery;

	public void setRecovery(DBInfoRecovery recovery) {
		this.recovery = recovery;
	}

	public void setDbPositionRecorder(DbInsPositionRecorder dbPositionRecorder) {
		this.dbPositionRecorder = dbPositionRecorder;
	}

	private List<EventType> watchEvent;
	private Map<String, WatchTableFilter> watchTableInfoFilter = new HashMap<>();

	public static DbInstanceInfo instance(DatabaseInitInfo initInfo) {
		initInfo.parseConfig();
		return new DbInstanceInfo().setDatabaseInitInfo(initInfo);
	}

	public DbInstanceInfo setDatabaseInitInfo(DatabaseInitInfo initInfo) {
		this.databaseInitInfo = initInfo;
		return this;
	}

	public boolean awareChangeType(ChangeType changeType) {
		return this.databaseInitInfo.awareChangeType(changeType);
	}

	public void refreshAllDbInfo() {
		try {
			List<DBInitInfo> initDBInfo = this.recovery.initDBInfo();
			initDBInfo.stream().flatMap(dbInitInfo -> {
				return dbInitInfo.getSubTableInfo().stream().map(subInfo -> this.initFilter(subInfo,
						dbInitInfo.getTableInfoMapper().get(subInfo.getSubTable())));
			}).forEach(filter -> {
				this.watchTableInfoFilter.put(filter.getKey(), filter);
			});
		} catch (SQLException e) {
			e.printStackTrace();
			// TODO raise exception
		}
	}

	public void refreshDbInfo(String db, String table) {
		try {
			DBInitInfo refreshDBInfo = this.recovery.refreshDBInfo(db);
			if (refreshDBInfo != null) {
				TableInfo findTableInfo = refreshDBInfo.findTableInfo(table);
				refreshDBInfo.getSubTableInfo().stream().filter(subInfo -> subInfo.getSubTable().contentEquals(table))
						.findAny().ifPresent(subTableInfo -> {
							this.watchTableInfoFilter.put(table, this.initFilter(subTableInfo, findTableInfo));
						});
			}
		} catch (SQLException e) {
			e.printStackTrace();
			// TODO raise exception
		}
	}

	private WatchTableFilter initFilter(SubTableInfoForServer subTableInfo, TableInfo tableInfo) {
		return new WatchTableFilter(tableInfo, subTableInfo);
	}

	public boolean needPublish(String holdName, Serializable[] columnCur, Serializable[] columnBefore) {
		return !this.watchTableInfoFilter.getOrDefault(holdName, WatchTableFilter.FALSE_FILTER).filter(columnCur,
				columnBefore);
	}

	public TableInfo getTableInfo(String db, String table) {
		return this.databaseInitInfo.findTableInfo(db, table);
	}

	public boolean containSubTable(String db, String table) {
		return this.databaseInitInfo.containSubTableInfo(db, table);
	}

	public void shutdownConnections() {
		this.connectionPool.release();
	}

}
