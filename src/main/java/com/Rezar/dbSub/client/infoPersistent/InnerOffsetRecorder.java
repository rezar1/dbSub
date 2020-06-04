package com.Rezar.dbSub.client.infoPersistent;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import com.Rezar.dbSub.base.dbInfo.TableMark;
import com.Rezar.dbSub.client.interfaces.OffsetRecorder;

public class InnerOffsetRecorder implements OffsetRecorder {

	private ConcurrentHashMap<TableMark, String> innerRecorder = new ConcurrentHashMap<TableMark, String>();

	@Override
	public void close() throws IOException {
		this.innerRecorder.clear();
	}

	@Override
	public String takeLastOffset(TableMark tableMark) {
		return this.innerRecorder.get(tableMark);
	}

	@Override
	public boolean saveLastOffset(TableMark tableMark, String seqId) {
		this.innerRecorder.put(tableMark, seqId);
		return true;
	}

}
