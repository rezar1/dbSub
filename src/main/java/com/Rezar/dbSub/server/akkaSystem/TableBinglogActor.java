package com.Rezar.dbSub.server.akkaSystem;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.Rezar.dbSub.base.enums.OffsetEnum;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SubTableBinlogActorMessage;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SubTableBinlogActorMessage.SyncBinlogMessageAdaptor;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SyncBinlogMessage;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SyncBinlogRespMessage;
import com.Rezar.dbSub.base.event.SyncEvent;
import com.Rezar.dbSub.server.interfaces.BinlogDataStorage;
import com.Rezar.dbSub.server.interfaces.DbInsPositionRecorder;
import com.Rezar.dbSub.utils.BinlogUtils;
import com.Rezar.dbSub.utils.GU;
import com.Rezar.dbSub.utils.ServerIpUtil;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
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
public final class TableBinglogActor extends AbstractBehavior<SubTableBinlogActorMessage> {

	private DbInsPositionRecorder posRecorder;
	// 记得关闭
	private BinlogDataStorage dataStorage;

	private List<ActorRef<SubTableBinlogActorMessage>> allClientActors = new ArrayList<>();

	private String dbMarkId;

	private TableBinglogActor(ActorContext<SubTableBinlogActorMessage> context, String dbIns, String db, String table,
			DbInsPositionRecorder posRecorder) {
		super(context);
		this.posRecorder = posRecorder;
		log.info("{} this posRecorder:{}", table, posRecorder == null);
		try {
			this.dataStorage = posRecorder.initTableDataStorage(dbIns, db, table);
		} catch (Exception e) {
			log.error("TableBinglogActor error:{}", e);
		}
		this.dbMarkId = dbIns + ":" + db + ":" + table;
		log.info("TableBinglogActor for {}:{} init over,and path:{}", db, table, this);
	}

	public static Behavior<SubTableBinlogActorMessage> create(String dbIns, String db, String table,
			DbInsPositionRecorder posRecorder) {
		// 加入监控策略,停止服务
		return Behaviors.setup(context -> new TableBinglogActor(context, dbIns, db, table, posRecorder));
	}

	@Override
	public Receive<SubTableBinlogActorMessage> createReceive() {
		return newReceiveBuilder().onMessage(SyncBinlogMessageAdaptor.class, this::handleSyncReq)
				.onMessage(SyncEvent.class, this::handleSyncEvent).onSignal(akka.actor.typed.Terminated.class, msg -> {
					log.info("Client stopped: {}", msg.getRef().path().name());
					this.allClientActors.remove(msg.getRef());
					return Behaviors.same();
				}).onSignal(PostStop.class, this::stop).build();
	}

	private Behavior<SubTableBinlogActorMessage> stop(PostStop msg) {
		IOUtils.closeQuietly(this.dataStorage);
		log.info("table:{} stoped", dbMarkId);
		return Behaviors.same();
	}

	private Behavior<SubTableBinlogActorMessage> handleSyncEvent(SyncEvent binlogEvent) {
		this.dataStorage.store(binlogEvent);
		if (binlogEvent.getSeqId() == null) {
			log.error("storage :{} not set storageId for syncEvent", this.dataStorage.getClass().getName());
			return Behaviors.stopped();
		}
		return Behaviors.same();
	}

	private Behavior<SubTableBinlogActorMessage> handleSyncReq(SyncBinlogMessageAdaptor msgAdaptor) {
		SyncBinlogMessage msg = msgAdaptor.msg;
		String offset = msg.offset;
		if (GU.isNullOrEmpty(offset)) {
			offset = OffsetEnum.CONTINUE.name();
		}
		String tryToBeginOffset = null;
		if (OffsetEnum.LAST_POS.name().equals(offset)) {
			tryToBeginOffset = this.posRecorder.readClientLastPos(msg.durableId.getBytes());
			log.info("client with durabldId:[{}] read last pos:{}", msg.durableId, tryToBeginOffset);
		} else if (!OffsetEnum.CONTINUE.name().equals(offset)) {
			// 从指定数据开始读
			if (!BinlogUtils.checkOffset(offset)) {
				msg.reqReplyTo.tell(SyncBinlogRespMessage.justResp(false, "offset illegal:[" + offset + "]"));
				return Behaviors.same();
			}
			tryToBeginOffset = offset;
			log.info("client with durabldId:[{}] read from pos:{}", msg.durableId, tryToBeginOffset);
		}
		spwanClientActor(msg.reqReplyTo, msg.clientActor, tryToBeginOffset, msg.durableId, msg.dbInstanceName);
		return Behaviors.same();
	}

	/**
	 * 初始化一个Actor专门处理和客户端的消息分发
	 * 
	 * @param binlogEventTo
	 * @param clientActor
	 * @param tryToBeginOffset
	 * @param durableId
	 * @param dbInstanceName
	 */
	private void spwanClientActor(ActorRef<SyncBinlogRespMessage> reqReplyTo,
			ActorRef<SubTableBinlogActorMessage> binlogEventTo, String tryToBeginOffset, String durableId,
			String dbInstanceName) {
		if (this.dataStorage.tooOldSeqId(tryToBeginOffset)) {
			reqReplyTo.tell(SyncBinlogRespMessage.justResp(false, "too old seqId:" + tryToBeginOffset
					+ ",please ignore the seqId corresponding to durableId [" + durableId
					+ "] under clientinfo of the client or restart the binlog service from the historical location,current binlog service's address:"
					+ ServerIpUtil.SERVER_IP));
			return;
		}
		ActorRef<SubTableBinlogActorMessage> clientConnectActor = super.getContext().spawn(
				ClientConnectActor.create(binlogEventTo, tryToBeginOffset, durableId, posRecorder, dataStorage),
				"ClientConnectActor:" + binlogEventTo.path().uid() + ":" + durableId);
		// 通知客户端请求的结果
		reqReplyTo.tell(SyncBinlogRespMessage.justResp(durableId, tryToBeginOffset, clientConnectActor));
		super.getContext().watch(clientConnectActor);
	}

}
