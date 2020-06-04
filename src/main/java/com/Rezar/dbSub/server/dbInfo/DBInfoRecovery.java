package com.Rezar.dbSub.server.dbInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.Rezar.dbSub.base.dbInfo.TableInfo;
import com.Rezar.dbSub.server.dbInfo.TableColumnList.ColumnDef;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import snaq.db.ConnectionPool;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2019年1月11日 下午4:20:01
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public class DBInfoRecovery {

	String tblSql = "SELECT TABLES.TABLE_NAME, CCSA.CHARACTER_SET_NAME " + "FROM INFORMATION_SCHEMA.TABLES "
			+ "JOIN information_schema.COLLATION_CHARACTER_SET_APPLICABILITY AS CCSA"
			+ " ON TABLES.TABLE_COLLATION = CCSA.COLLATION_NAME WHERE TABLES.TABLE_SCHEMA = ?";

	String columnSql = "SELECT TABLE_NAME,COLUMN_NAME, DATA_TYPE, CHARACTER_SET_NAME, ORDINAL_POSITION, "
			+ "COLUMN_TYPE, DATETIME_PRECISION, COLUMN_KEY "
			+ "FROM `information_schema`.`COLUMNS` WHERE TABLE_SCHEMA = ? ORDER BY TABLE_NAME, ORDINAL_POSITION";

	@Getter
	private DatabaseInitInfo databaseInfo;
	private ConnectionPool dbConnPool;

	public DBInfoRecovery(DatabaseInitInfo databaseInfo, ConnectionPool dbConnPool) {
		this.databaseInfo = databaseInfo;
		this.dbConnPool = dbConnPool;
	}

	public DBInitInfo refreshDBInfo(String db) throws SQLException {
		DBInitInfo dbInfo = this.databaseInfo.findDBInfo(db);
		if (dbInfo != null) {
			this.refreshDBInfo(dbInfo);
		} else {
			log.info("can not find dbInfo with dbName:{}", db);
		}
		return dbInfo;
	}

	public List<DBInitInfo> initDBInfo() throws SQLException {
		List<DBInitInfo> subDBInitInfos = databaseInfo.getSubDBInitInfos();
		for (DBInitInfo dbInitInfo : subDBInitInfos) {
			refreshDBInfo(dbInitInfo);
			dbInitInfo.initSubTableMapper();
		}
		return subDBInitInfos;
	}

	public void refreshDBInfo(DBInitInfo dbInitInfo) throws SQLException {
		String dbName = dbInitInfo.getDb();
		Map<String, TableInfo> tableInfoMap = new HashMap<>();
		try (Connection connection = dbConnPool.getConnection()) {
			PreparedStatement tablePreparedStatement = connection.prepareStatement(tblSql);
			tablePreparedStatement.setString(1, dbName);
			ResultSet rs = tablePreparedStatement.executeQuery();
			while (rs.next()) {
				String tableName = rs.getString("TABLE_NAME");
				String characterSetName = rs.getString("CHARACTER_SET_NAME");
				TableInfo t = new TableInfo(dbName, tableName, characterSetName);
				tableInfoMap.put(tableName, t);
			}
			rs.close();
		}
		try (Connection connection = dbConnPool.getConnection()) {
			PreparedStatement columnPreparedStatement = connection.prepareStatement(columnSql);
			columnPreparedStatement.setString(1, dbName);
			ResultSet r = columnPreparedStatement.executeQuery();
			while (r.next()) {
				String tableName = r.getString("TABLE_NAME");
				if (tableInfoMap.containsKey(tableName)) {
					TableInfo t = tableInfoMap.get(tableName);
					String colName = r.getString("COLUMN_NAME");
					String colType = r.getString("DATA_TYPE");
					t.addColumnDef(new ColumnDef(colName, t.getCharset(), colType));
				}
			}
			r.close();
		}
		dbInitInfo.setTableInfoMapper(tableInfoMap);
	}

	public Long getServerID() throws SQLException {
		long serverId = 0;
		try (Connection c = this.dbConnPool.getConnection()) {
			ResultSet rs = c.createStatement().executeQuery("SELECT @@server_id as server_id");
			if (!rs.next()) {
				throw new RuntimeException("Could not retrieve server_id!");
			}
			serverId = rs.getLong("server_id");
		}
		return serverId;
	}

	/**
	 * 
	 */
	public void closeConnection() {
		this.dbConnPool.release();
	}

}
