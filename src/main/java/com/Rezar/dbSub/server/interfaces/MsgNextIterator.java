package com.Rezar.dbSub.server.interfaces;

import com.Rezar.dbSub.base.event.SyncEvent;

public interface MsgNextIterator {

	public SyncEvent blockingTake() throws InterruptedException;

	public void close();

}
