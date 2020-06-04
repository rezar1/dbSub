package com.Rezar.dbSub.server.akkaSystem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.DB_CONTROLLER;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SubTableBinlogActorMessage;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SubTableBinlogActorMessage.SyncBinlogMessageAdaptor;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SyncBinlogMessage;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SyncBinlogRespMessage;
import com.Rezar.dbSub.base.event.SyncEvent;
import com.Rezar.dbSub.server.dbInfo.DbInstanceInfo;
import com.Rezar.dbSub.server.infoPersistent.DataStoreInfo;
import com.Rezar.dbSub.server.interfaces.DbInsPositionRecorder;
import com.Rezar.dbSub.server.relay.BinlogProducer;
import com.Rezar.dbSub.utils.BinlogUtils;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import lombok.extern.slf4j.Slf4j;

/**
 * 1) 处理单DB binlog 事件的监听和下发 <br/>
 * 2) 处理客户端链接和消息转发给指定的table处理actor <br/>
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 12, 2020 2:21:25 PM
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public final class SingleDbBinglogActor extends BinlogProducer {

	public final ActorContext<SingleDbActorMessage> context;
	private final ActorRef<SingleDbActorMessage> selfMsgRef;

	public String curDbInsSeqId;

	public static Behavior<SingleDbActorMessage> create(DbInstanceInfo dbInstanceInfo) {
		return Behaviors.setup(context -> {
			return new SingleDbBinglogActor(context, dbInstanceInfo).process();
		});
	}

	private final Map<String, SubTableActorWrapper> subTableWrapper = new HashMap<>();

	public SingleDbBinglogActor(ActorContext<SingleDbActorMessage> context, DbInstanceInfo dbInstanceInfo) {
		super(dbInstanceInfo);
		this.context = context;
		this.selfMsgRef = this.context.messageAdapter(SingleDbActorMessage.class, msg -> msg);
		// 初始化多个子subtable的Actor
		super.streamOfSubTable().map(tableInfo -> {
			DbInsPositionRecorder posRecorder = DataStoreInfo.STORE
					.findPositionRecorder(dbInstanceInfo.getDatabaseInitInfo().getDbInstanceName());
			return new SubTableActorWrapper(tableInfo.getDb(), tableInfo.getTableName(),
					context.spawn(
							TableBinglogActor.create(dbInstanceInfo.getDatabaseInitInfo().getDbInstanceName(),
									tableInfo.getDb(), tableInfo.getTableName(), posRecorder),
							String.format("SubTableActor:%s:%s", tableInfo.getDb(), tableInfo.getTableName())));
		}).forEach(wrapper -> {
			this.subTableWrapper.put(String.format("%s:%s", wrapper.db, wrapper.table), wrapper);
		});
		try {
			super.startBinlog();
			log.info("SingleDbBinglogActor start success");
		} catch (IOException e) {
			e.printStackTrace();
			// TODO 告警
		}
	}

	private Behavior<SingleDbActorMessage> process() {
		return Behaviors.receive(SingleDbActorMessage.class).onMessageEquals(DB_CONTROLLER.INSTANCE, () -> {
			log.info("receive stop msg:{}", this.dbInstanceInfo.getDatabaseInitInfo().getDbInstanceName());
			// 接收到停机事件后,先停止binlog订阅,再在postStop中清理后续资源
			this.stopPre();
			return Behaviors.stopped();
		}).onMessage(SyncBinlogMessage.class, this::handleSyncBinlogMessage).onAnyMessage(msg -> {
			log.info("any msg is :{}", msg);
			return Behaviors.same();
		}).onSignal(PostStop.class, stop -> {
			this.stopLast();
			return Behaviors.same();
		}).build();
	}

	private void stopPre() {
		super.stopBinlog();
	}

	private void stopLast() {
		this.dbInstanceInfo.shutdownConnections();
		DataStoreInfo.STORE.shutdownRecorder(this.dbInstanceInfo.getDatabaseInitInfo());
		log.info("dbInstanceInfo:{} stoped", dbInstanceInfo.getDatabaseInitInfo().getDbInstanceName());
	}

	/**
	 * 处理客户端的binlog同步请求
	 * 
	 * @param msg
	 * @return
	 */
	private Behavior<SingleDbActorMessage> handleSyncBinlogMessage(SyncBinlogMessage msg) {
		String dbAndTable = BinlogUtils.dbAndTable(msg.db, msg.tableName);
		log.info("handleSyncBinlogMessage:{}:{}", msg, this.subTableWrapper.containsKey(dbAndTable));
		if (this.subTableWrapper.containsKey(dbAndTable)) {
			this.subTableWrapper.get(dbAndTable).tell(new SyncBinlogMessageAdaptor(msg));
		} else {
			msg.reqReplyTo.tell(SyncBinlogRespMessage.justResp(false, dbAndTable + " 不存在"));
		}
		return Behaviors.same();
	}

	@Override
	protected void publishEvent(SyncEvent buildEvent) {
		String dbAndTable = buildEvent.dbAndTable();
		this.curDbInsSeqId = buildEvent.getSeqId();
		if (this.subTableWrapper.containsKey(dbAndTable)) {
			subTableWrapper.get(dbAndTable).tell(buildEvent);
		}
	}

	static class SubTableActorWrapper {
		final String db;
		final String table;
		final ActorRef<SubTableBinlogActorMessage> subTableActor;

		public SubTableActorWrapper(String db, String table, ActorRef<SubTableBinlogActorMessage> subTableActor) {
			super();
			this.db = db;
			this.table = table;
			this.subTableActor = subTableActor;
		}

		public void tell(SubTableBinlogActorMessage binlogEventMessage) {
			this.subTableActor.tell(binlogEventMessage);
		}

	}

	@Override
	protected Void onBinlogException(Throwable ex) {
		log.info("error while start binlogClient with:{}", this.dbInstanceInfo.getDatabaseInitInfo());
		log.error("onBinlogException:{}", ex);
		selfMsgRef.tell(DB_CONTROLLER.INSTANCE);
		return null;
	}

}
