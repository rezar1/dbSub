package com.Rezar.dbSub.server.infoPersistent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import org.apache.commons.io.IOUtils;

import com.Rezar.dbSub.server.dbInfo.DatabaseInitInfo;
import com.Rezar.dbSub.server.interfaces.DbInsPositionRecorder;

import lombok.extern.slf4j.Slf4j;

/**
 * 基于levelDB存储数据的目录以及相关存储器获取的方法
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 21, 2020 12:35:59 PM
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public class DataStoreInfo {

	public static final DataStoreInfo STORE = new DataStoreInfo();

	private DataStoreInfo() {

	}

	private Map<String, DbInsPositionRecorder> dbPositionRecorderMap = new HashMap<>();

	private static Supplier<DbInsPositionRecorder> storeSupplier = null;

	static {
		storeSupplier = withConfig();
		if (storeSupplier == null) {
			storeSupplier = () -> new PositionRecorderWithLevelDB();
		}
	}

	public void shutdownRecorder(DatabaseInitInfo databaseInitInfo) {
		DbInsPositionRecorder findPositionRecorder = this.findPositionRecorder(databaseInitInfo.getDbInstanceName());
		if (findPositionRecorder != null) {
			IOUtils.closeQuietly(findPositionRecorder);
		}
	}

	public DbInsPositionRecorder initDbPositionRecorder(DatabaseInitInfo databaseInfo, long serverId) {
		if (this.dbPositionRecorderMap.containsKey(databaseInfo.getDbInstanceName())) {
			return this.dbPositionRecorderMap.get(databaseInfo.getDbInstanceName());
		}
		DbInsPositionRecorder recorder = storeSupplier.get();
		recorder.initPositionRecorder(databaseInfo, serverId);
		this.dbPositionRecorderMap.put(databaseInfo.getDbInstanceName(), recorder);
		return recorder;
	}

	public DbInsPositionRecorder findPositionRecorder(String dbIns) {
		return this.dbPositionRecorderMap.get(dbIns);
	}

	public void onClose() {
		dbPositionRecorderMap.values().forEach(t -> {
			try {
				t.close();
			} catch (IOException e) {
				log.error("error while close PositionRecorder:{}", e);
			}
		});
	}

	/**
	 * 看能不能配置
	 * 
	 * @return
	 */
	private static Supplier<DbInsPositionRecorder> withConfig() {
		return null;
	}

}
