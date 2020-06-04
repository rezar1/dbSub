package com.Rezar.dbSub.server.interfaces;

import java.io.Closeable;

import com.Rezar.dbSub.server.dbInfo.BinlogPosition;
import com.Rezar.dbSub.server.dbInfo.DatabaseInitInfo;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月25日 上午12:54:26
 * @Desc 些年若许,不负芳华.
 *
 */
public interface DbInsPositionRecorder extends Closeable {

	public void setFileName(String filename);

	public void setPosition(long position);

	public BinlogPosition recoverFromStore();

	public void storeClientCurrentPos(byte[] durId, String offset);

	public String readClientLastPos(byte[] durId);

	public void initPositionRecorder(DatabaseInitInfo databaseInfo, long serverId);

	public BinlogDataStorage initTableDataStorage(String dbIns, String db, String table);

}
