package com.Rezar.dbSub.server.dbInfo;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.Rezar.dbSub.server.infoPersistent.DataStoreInfo;
import com.Rezar.dbSub.server.interfaces.DbInsPositionRecorder;

import lombok.extern.slf4j.Slf4j;
import snaq.db.ConnectionPool;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月25日 上午12:37:10
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public class DBInfoResolver {

	public static void main(String[] args) throws SQLException {
		ConnectionPool dbConnPool = new ConnectionPool("", 2, 10, 10, 10, "jdbc:mysql://127.0.0.1:3306", "root",
				"root");
		dbConnPool.getConnection();
	}

	public static Stream<DbInstanceInfo> doInit(List<DatabaseInitInfo> dbInitInfoList) {
		return dbInitInfoList.parallelStream().map(DBInfoResolver::parseToDbInstanceInfo).filter(Objects::nonNull)
				.collect(Collectors.toList()).stream();
	}

	public static DbInstanceInfo parseToDbInstanceInfo(DatabaseInitInfo databaseInfo) {
		try {
			ConnectionPool dbConnPool = new ConnectionPool(databaseInfo.getClientId(), 10, 10, 10,
					databaseInfo.getUrl(), databaseInfo.getUsername(), databaseInfo.getPassword());
			DbInstanceInfo instance = DbInstanceInfo.instance(databaseInfo);
			DBInfoRecovery recovery = new DBInfoRecovery(databaseInfo, dbConnPool);
			recovery.initDBInfo();
			long serverId = recovery.getServerID();
			DbInsPositionRecorder positionRecord = DataStoreInfo.STORE.initDbPositionRecorder(databaseInfo, serverId);
			instance.setConnectionPool(dbConnPool);
			instance.setRecovery(recovery);
			instance.setDbPositionRecorder(positionRecord);
			return instance;
		} catch (SQLException e) {
			log.error("error while initSingleDbInfo:{}", e);
		}
		return null;
	}

}
