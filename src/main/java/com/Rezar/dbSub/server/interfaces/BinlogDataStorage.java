package com.Rezar.dbSub.server.interfaces;

import java.io.Closeable;

import com.Rezar.dbSub.base.event.SyncEvent;

public interface BinlogDataStorage extends Closeable {

	public void init(String dbIns, String db, String table);

	public void store(SyncEvent event);

	public boolean tooOldSeqId(String fromOffset);

	public MsgNextIterator iteratorFrom(String offset);

}
