package com.Rezar.dbSub.base.event;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringExclude;

import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SubTableBinlogActorMessage;
import com.Rezar.dbSub.utils.JacksonUtil;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;

/**
 * 
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 11, 2020 6:06:28 PM
 * @Desc 些年若许,不负芳华.
 *
 */
@Data
public class SyncEvent implements SubTableBinlogActorMessage {

	public String db;
	public String table;
	public int eventType;
	public long timestamp;
	public Map<String, Object> oldDatas;
	public Map<String, Object> newDatas;

	private String seqId;
	@JsonIgnore
	@ToStringExclude
	private byte[] sourceData;

	public SyncEvent() {
	}

	public byte[] getSourceData() {
		return this.sourceData;
	}

	public SyncEvent(byte[] sourceData) {
		this.sourceData = sourceData;
	}

	public SyncEvent(String db, String table, int eventType, long timestamp, HashMap<String, Object> oldDatas,
			HashMap<String, Object> newDatas) {
		this(null, db, table, eventType, timestamp, oldDatas, newDatas);
	}

	public SyncEvent(String seqId, String db, String table, int eventType, long timestamp,
			HashMap<String, Object> oldDatas, HashMap<String, Object> newDatas) {
		super();
		this.seqId = seqId;
		this.db = db;
		this.table = table;
		this.eventType = eventType;
		this.timestamp = timestamp;
		this.oldDatas = oldDatas;
		this.newDatas = newDatas;
	}

	public String encodeJson() {
		return JacksonUtil.obj2Str(this);
	}

	public String dbAndTable() {
		return this.db + ":" + this.table;
	}

	public static SyncEvent nothing() {
		return new SyncEvent();
	}

}
