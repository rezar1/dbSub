package com.Rezar.dbSub.server.infoPersistent;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import com.Rezar.dbSub.base.ServerConstants;
import com.Rezar.dbSub.server.dbInfo.BinlogPosition;
import com.Rezar.dbSub.server.dbInfo.DatabaseInitInfo;
import com.Rezar.dbSub.server.interfaces.BinlogDataStorage;
import com.Rezar.dbSub.server.interfaces.DbInsPositionRecorder;

import lombok.extern.slf4j.Slf4j;

/**
 * 记录或读取系统运行需要的数据:<br/>
 * 
 * 针对单数据库实例而言的
 * 
 * 1) 读写 (binlog 文件和binlogOffset)
 * 
 * 2) 读写 客户端LAST_READ_POS
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 21, 2020 12:46:58 PM
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public class PositionRecorderWithLevelDB implements DbInsPositionRecorder {

	private DB db;

	private static final byte[] FILE_KEY = "FILE_KEY".getBytes();
	private static final byte[] POS_KEY = "POS_KEY".getBytes();

	@Override
	public void close() throws IOException {
		this.db.close();
	}

	@Override
	public void setFileName(String filename) {
		this.db.put(FILE_KEY, filename.getBytes());
	}

	@Override
	public void setPosition(long position) {
		this.db.put(POS_KEY, String.valueOf(position).getBytes());
	}

	@Override
	public BinlogPosition recoverFromStore() {
		byte[] posBytes = this.db.get(POS_KEY);
		byte[] fileNameBytes = this.db.get(FILE_KEY);
		String fileName = null;
		long offset = 0;
		if (fileNameBytes != null && fileNameBytes.length != 0) {
			fileName = new String(fileNameBytes);
		}
		if (posBytes != null && posBytes.length != 0) {
			offset = Long.parseLong(new String(posBytes));
		}
		return new BinlogPosition(offset, fileName);
	}

	@Override
	public void storeClientCurrentPos(byte[] durId, String offset) {
		this.db.put(durId, offset.getBytes());
	}

	@Override
	public String readClientLastPos(byte[] durId) {
		byte[] bs = this.db.get(durId);
		return bs == null ? null : new String(bs);
	}

	@Override
	public void initPositionRecorder(DatabaseInitInfo databaseInfo, long serverId) {
		File baseFile = new File(
				ServerConstants.SAVE_BASE_FILE.getAbsolutePath() + File.separator + databaseInfo.getDbInstanceName());
		try {
			Options options = new Options();
			options.createIfMissing(true);
			log.info("baseFile path:{}", baseFile.getAbsolutePath());
			this.db = factory.open(baseFile, options);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalStateException(
					String.format("databaseInstance:%s with serverId:%s can not init posRecorder",
							databaseInfo.getDbInstanceName(), serverId));
		}
	}

	@Override
	public BinlogDataStorage initTableDataStorage(String dbIns, String db, String table) {
		BinlogDataStorageWithLevelDB storage = new BinlogDataStorageWithLevelDB();
		storage.init(dbIns, db, table);
		return storage;
	}

}
