package com.Rezar.dbSub.server.relay;

import java.io.IOException;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import com.Rezar.dbSub.base.dbInfo.TableInfo;
import com.Rezar.dbSub.base.enums.ChangeType;
import com.Rezar.dbSub.base.event.SyncEvent;
import com.Rezar.dbSub.server.dbInfo.BinlogPosition;
import com.Rezar.dbSub.server.dbInfo.DBInitInfo;
import com.Rezar.dbSub.server.dbInfo.DatabaseInitInfo;
import com.Rezar.dbSub.server.dbInfo.DbInstanceInfo;
import com.Rezar.dbSub.utils.GU;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.BinaryLogClient.AbstractLifecycleListener;
import com.github.shyiko.mysql.binlog.BinaryLogClient.EventListener;
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData;
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData;
import com.github.shyiko.mysql.binlog.event.deserialization.EventDeserializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class BinlogProducer extends AbstractLifecycleListener implements EventListener {

	protected final BinaryLogClient binlogClient;

	private ConcurrentHashMap<Long, String> tableIdToTableNameMapper = new ConcurrentHashMap<>();
	private static final String subHoldNameFormat = "%s:%s"; // db:table
	public volatile String binlogFileNameIndex; // mysql-bin.000005后面的5

	protected final DbInstanceInfo dbInstanceInfo;

	public BinlogProducer(DbInstanceInfo dbInstanceInfo) {
		this.dbInstanceInfo = dbInstanceInfo;
		DatabaseInitInfo databaseInfo = this.dbInstanceInfo.getDatabaseInitInfo();
		binlogClient = new BinaryLogClient(databaseInfo.getHost(), databaseInfo.getPort(), databaseInfo.getUsername(),
				databaseInfo.getPassword());
		binlogClient.setServerId(Integer.parseInt(databaseInfo.getClientId()));
		BinlogPosition recoverFromStore = this.dbInstanceInfo.getDbPositionRecorder().recoverFromStore();
		log.info("databaseInfo:{} recoverPosFromStore:{}", databaseInfo.getDbInstanceName(), recoverFromStore);
		if (databaseInfo.isGtidMode()) {
			binlogClient.setGtidSet(recoverFromStore.getGtidSetStr());
		} else {
			String fromFile = recoverFromStore.getFile();
			long fromOffset = recoverFromStore.getOffset();
			// 如果是第一次启动服务,并且指定了订阅开始的文件和下标,则覆盖
			if (fromFile == null && (GU.notNullAndEmpty(databaseInfo.getFromBinlogFile())
					&& GU.notNullAndEmpty(databaseInfo.getFromBinlogOffset()))) {
				fromFile = databaseInfo.getFromBinlogFile();
				fromOffset = Long.parseLong(databaseInfo.getFromBinlogOffset());
				log.info("{}: init from configXml:[{}:{}]", databaseInfo.getDbInstanceName(), fromFile, fromOffset);
			}
			binlogClient.setBinlogFilename(fromFile);
			binlogClient.setBinlogPosition(fromOffset);
		}
		binlogClient.registerLifecycleListener(this);
		binlogClient.registerEventListener(this);
	}

	@Override
	public void onEvent(Event event) {
		updateClientBinlogFilenameAndPosition(event);
		EventHeaderV4 header = event.getHeader();
		long curPos = header.getPosition();
		EventType eventType = header.getEventType();
		long timestamp = header.getTimestamp();
		ChangeType changeEventType = ChangeType.byEventType(eventType);
		if (changeEventType != null && this.dbInstanceInfo.awareChangeType(changeEventType)) {
			EventData data = event.getData();
			String holdName = null;
			int index = 0;
			if (changeEventType == ChangeType.DELETE) {
				DeleteRowsEventData dred = (DeleteRowsEventData) data;
				holdName = this.tableIdToTableNameMapper.get(dred.getTableId());
				if (!this.needHandle(holdName)) {
					return;
				}
				for (Serializable[] columnBeforeItem : dred.getRows()) {
					SyncEvent buildEvent = this.buildEvent(holdName, timestamp, changeEventType, null,
							columnBeforeItem);
					if (buildEvent == null) {
						continue;
					}
					String seqId = curPos + "_" + (index++);
					buildEvent.setSeqId(this.binlogFileNameIndex + "_" + seqId);
					this.publishEvent(buildEvent);
				}
			} else if (changeEventType == ChangeType.UPDATE) {
				UpdateRowsEventData ured = (UpdateRowsEventData) data;
				holdName = this.tableIdToTableNameMapper.get(ured.getTableId());
				if (!this.needHandle(holdName)) {
					return;
				}
				for (Entry<Serializable[], Serializable[]> item : ured.getRows()) {
					SyncEvent buildEvent = this.buildEvent(holdName, timestamp, changeEventType, item.getValue(),
							item.getKey());
					if (buildEvent == null) {
						continue;
					}
					String seqId = curPos + "_" + (index++);
					buildEvent.setSeqId(this.binlogFileNameIndex + "_" + seqId);
					this.publishEvent(buildEvent);
				}
			} else if (changeEventType == ChangeType.INSERT) {
				WriteRowsEventData wred = (WriteRowsEventData) data;
				holdName = this.tableIdToTableNameMapper.get(wred.getTableId());
				if (!this.needHandle(holdName)) {
					return;
				}
				for (Serializable[] itemCur : wred.getRows()) {
					SyncEvent buildEvent = this.buildEvent(holdName, timestamp, changeEventType, itemCur, null);
					if (buildEvent == null) {
						continue;
					}
					String seqId = curPos + "_" + (index++);
					buildEvent.setSeqId(this.binlogFileNameIndex + "_" + seqId);
					this.publishEvent(buildEvent);
				}
			} else {
				return;
			}
		} else {
			// 表结构修改|表添加 等事件处理 -->更新内存等操作
			checkForTableAlter(eventType, event);
		}
	}

	// 子类处理实际的事件分发
	protected abstract void publishEvent(SyncEvent buildEvent);

	private SyncEvent buildEvent(String dbAndTableMark, long timestamp, ChangeType changeEventType,
			Serializable[] columnCur, Serializable[] columnBefore) {
		// 判断过滤掉无关字段后数据是否有变更
		boolean needPublish = dbInstanceInfo.needPublish(dbAndTableMark, columnCur, columnBefore);
		if (!needPublish) {
			return null;
		}
		String[] dbAndTable = dbAndTableMark.split(":");
		TableInfo tableInfo = dbInstanceInfo.getTableInfo(dbAndTable[0], dbAndTable[1]);
		HashMap<String, Object> parseToValueOfCur = null;
		if (columnCur != null) {
			parseToValueOfCur = tableInfo.parseToValue(columnCur);
		}
		HashMap<String, Object> parseToValueOfBefore = null;
		if (columnBefore != null) {
			parseToValueOfBefore = tableInfo.parseToValue(columnBefore);
		}
		SyncEvent event = new SyncEvent(dbAndTable[0], dbAndTable[1], changeEventType.type, timestamp,
				parseToValueOfBefore, parseToValueOfCur);
		return event;
	}

	private void updateClientBinlogFilenameAndPosition(Event event) {
		EventHeader eventHeader = event.getHeader();
		EventType eventType = eventHeader.getEventType();
		if (eventType == EventType.ROTATE) {
			EventData eventData = event.getData();
			RotateEventData rotateEventData;
			if (eventData instanceof EventDeserializer.EventDataWrapper) {
				rotateEventData = (RotateEventData) ((EventDeserializer.EventDataWrapper) eventData).getInternal();
			} else {
				rotateEventData = (RotateEventData) eventData;
			}
			this.dbInstanceInfo.getDbPositionRecorder().setFileName(rotateEventData.getBinlogFilename());
			this.dbInstanceInfo.getDbPositionRecorder().setPosition(rotateEventData.getBinlogPosition());
			updateBinlogFileName(rotateEventData.getBinlogFilename());
		} else if (eventType == EventType.TABLE_MAP) {
			TableMapEventData data = event.getData();
			if (!tableIdToTableNameMapper.containsKey(data.getTableId())) {
				String holdName = String.format(subHoldNameFormat, data.getDatabase(), data.getTable());
				this.tableIdToTableNameMapper.putIfAbsent(data.getTableId(), holdName);
			}
		} else if (eventType != EventType.TABLE_MAP && eventHeader instanceof EventHeaderV4) {
			EventHeaderV4 trackableEventHeader = (EventHeaderV4) eventHeader;
			long nextBinlogPosition = trackableEventHeader.getNextPosition();
			if (nextBinlogPosition > 0) {
				this.dbInstanceInfo.getDbPositionRecorder().setPosition(nextBinlogPosition);
			}
		}
	}

	private void updateBinlogFileName(String binlogFilename) {
		// 如 xxxx.000003 的文件
		this.binlogFileNameIndex = binlogFilename.replaceAll(".*?(0+)?(\\d+)", "$2");
	}

	private static Pattern alterTablePattern = Pattern.compile("^ALTER\\s+TABLE\\s+`(.*?)`\\.`(.*?)`",
			Pattern.CASE_INSENSITIVE);

	private void checkForTableAlter(EventType eventType, Event event) {
		if (eventType == EventType.QUERY) {
			QueryEventData queryEventData = event.getData();
			String sql = queryEventData.getSql();
			Matcher matcher = alterTablePattern.matcher(sql);
			if (matcher.find()) {
				String database = matcher.group(1);
				String tableName = matcher.group(2);
				log.info("try to update for db:{} table:{} and sql:{}", database, tableName, sql);
				if (this.dbInstanceInfo.containSubTable(database, tableName)) {
					try {
						this.dbInstanceInfo.getRecovery().refreshDBInfo(database);
					} catch (SQLException e) {
						log.error("error while refreshDBInfo:{} ", e);
					}
					log.info("success to update table:{}-{}", database, tableName);
				}
			}
		}
	}

	protected void startBinlog() throws IOException {
		CompletableFuture.runAsync(() -> {
			try {
				this.binlogClient.connect();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}).exceptionally(this::onBinlogException);
	}

	protected abstract Void onBinlogException(Throwable ex);

	protected void stopBinlog() {
		try {
			if (this.binlogClient != null && this.binlogClient.isConnected()) {
				this.binlogClient.disconnect();
			}
		} catch (Exception e) {
			log.info("error while topBinlogServer:{}", this.dbInstanceInfo.getDatabaseInitInfo().getDbInstanceName());
			log.error("error topBinlogServer:{}", e);
		}
	}

	@Override
	public void onConnect(BinaryLogClient client) {
		this.updateBinlogFileName(client.getBinlogFilename());
	}

	protected Stream<TableInfo> streamOfSubTable() {
		return this.dbInstanceInfo.getDatabaseInitInfo().getSubDBInitInfos().stream()
				.flatMap(DBInitInfo::streamSubTableToTableInfo);
	}

	/**
	 * 判断是否需要处理该表的binlog
	 * 
	 * @param tableMark
	 * @return
	 */
	protected boolean needHandle(String tableMark) {
		String[] dbAndTable = tableMark.split(":");
		return this.dbInstanceInfo.containSubTable(dbAndTable[0], dbAndTable[1]);
	}

}
