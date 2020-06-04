package com.Rezar.dbSub.client.infoPersistent;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import com.Rezar.dbSub.base.ServerConstants;
import com.Rezar.dbSub.base.dbInfo.TableMark;
import com.Rezar.dbSub.client.interfaces.OffsetRecorder;

import lombok.extern.slf4j.Slf4j;

/**
 * 客户端存储已消费的事件的seqId
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 23, 2020 6:03:42 PM
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public class OffsetRecorderByLevelDB implements OffsetRecorder {

	private DB db;

	public OffsetRecorderByLevelDB() {
		File baseFile = new File(ServerConstants.SAVE_BASE_FILE.getAbsolutePath() + File.separator + "clientInfo");
		try {
			Options options = new Options();
			options.createIfMissing(true);
			log.info("baseFile path:{}", baseFile.getAbsolutePath());
			this.db = factory.open(baseFile, options);
		} catch (IOException e) {
			log.error("OffsetRecorderByLevelDB:{}", e);
			throw new IllegalStateException("can not init clientInfo store db");
		}
	}

	@Override
	public String takeLastOffset(TableMark tableMark) {
		byte[] bs = this.db.get(tableMark.getBinaryMarkId());
		return bs != null ? new String(bs) : "";
	}

	@Override
	public boolean saveLastOffset(TableMark tableMark, String seqId) {
		this.db.put(tableMark.getBinaryMarkId(), seqId.getBytes());
		return true;
	}

	@Override
	public void close() throws IOException {
		this.db.close();
	}

}
