package com.Rezar.dbSub.client.akkaSystem;

import java.util.List;

import org.apache.commons.io.IOUtils;

import com.Rezar.dbSub.base.dbInfo.TableMark;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SubTableBinlogActorMessage;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SubTableBinlogActorMessage.BinlogEventAckMessage;
import com.Rezar.dbSub.base.event.SyncEvent;
import com.Rezar.dbSub.client.config.DisruptorConfig;
import com.Rezar.dbSub.client.config.TableBinlogProcessor;
import com.Rezar.dbSub.client.interfaces.OffsetRecorder;
import com.Rezar.dbSub.server.akkaSystem.ClientConnectActor.Start;
import com.Rezar.dbSub.server.akkaSystem.ClientConnectActor.Stop;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import lombok.extern.slf4j.Slf4j;

/**
 * 单表binlog处理
 * 
 * 1) 维护统一的offset记录器
 * 
 * 2) 投递消息到各个业务处理器上
 * 
 * 3) 处理异常
 * 
 * 配置mailBox为:MailboxSelector.fromConfig("client.bounded-mailbox");
 * 
 * 阻塞邮箱类型
 * 
 * 服务端持续发送消息直至阻塞
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 23, 2020 2:52:22 PM
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public class SingleTableBinlogClientActor extends AbstractBehavior<SubTableBinlogActorMessage>
		implements EventHandler<SyncEvent> {

	private TableMark tableMark;
	// 单表的所有事件处理器
	private List<TableBinlogProcessor> allProcessor;
	// 用于回复远端当前已处理下发的事件
	private volatile ActorRef<SubTableBinlogActorMessage> ackRef;

	private OffsetRecorder offsetRecorder;

	/**
	 * 接收服务器端事件并:
	 * 
	 * 1) 分发
	 * 
	 * 2) 记录
	 * 
	 * 3) ack
	 */
	private Disruptor<SyncEvent> disruptor;

	public SingleTableBinlogClientActor(ActorContext<SubTableBinlogActorMessage> context,
			List<TableBinlogProcessor> allProcessor, OffsetRecorder offsetRecorder, TableMark tableMark) {
		super(context);
		this.tableMark = tableMark;
		this.allProcessor = allProcessor;
		this.offsetRecorder = offsetRecorder;
		this.disruptor = initDisrupt();
	}

	@SuppressWarnings("unchecked")
	private Disruptor<SyncEvent> initDisrupt() {
		int bufferSize = 1024;
		Disruptor<SyncEvent> disruptor = new Disruptor<>(DisruptorConfig.eventInitFacotry, bufferSize,
				DisruptorConfig.threadFactory, ProducerType.SINGLE, new LiteBlockingWaitStrategy());
		disruptor.handleEventsWith(this.allProcessor.toArray(new EventHandler[] {})).then(this);
		disruptor.start();
		return disruptor;
	}

	public static Behavior<SubTableBinlogActorMessage> create(List<TableBinlogProcessor> allProcessor,
			OffsetRecorder offsetRecorder, TableMark tableMark) {
		return Behaviors
				.setup(context -> new SingleTableBinlogClientActor(context, allProcessor, offsetRecorder, tableMark));
	}

	@Override
	public Receive<SubTableBinlogActorMessage> createReceive() {
		return newReceiveBuilder().onMessageEquals(ClientController.STOP_CLIENT, () -> {
			// 停机事件,清理资源退出
			this.onStop();
			log.info("{} stop connect to remote server:{} ", this.tableMark, this.ackRef);
			return Behaviors.stopped();
		}).onMessageEquals(ClientController.START_CLIENT, () -> {
			assert this.ackRef != null;
			log.info("{} start connect to remote server:{} ", this.tableMark, this.ackRef);
			// 告知远端可以开始同步了
			this.ackRef.tell(Start.INSTANCE);
			return this;
		}).onMessage(InitClient.class, msg -> {
			log.info("{} init connect to remote server:{} from offset:{}", this.tableMark, msg.ackRef, msg.fromOffset);
			// 更新当前ack回复的远端Actor
			this.ackRef = msg.ackRef;
			return this;
		}).onMessage(SyncEvent.class, this::publishEvent).onSignal(PostStop.class, stop -> {
			IOUtils.closeQuietly(offsetRecorder);
			return Behaviors.same();
		}).build();
	}

	// 新的binlog事件
	public Behavior<SubTableBinlogActorMessage> publishEvent(SyncEvent binlogEvent) {
		this.disruptor.publishEvent(DisruptorConfig.TRANS_INSTANCE, binlogEvent);
		return this;
	}

	public void onStop() {
		if (this.ackRef != null) {
			this.ackRef.tell(Stop.INSTANCE);
		}
		this.disruptor.shutdown();
	}

	@Override
	public void onEvent(SyncEvent event, long sequence, boolean endOfBatch) throws Exception {
		// 更新客户端当前最新处理的offset
		this.offsetRecorder.saveLastOffset(tableMark, event.getSeqId());
		// 发送ack消息到远端
		this.ackRef.tell(new BinlogEventAckMessage(event.getSeqId()));
	}

	public static enum ClientController implements SubTableBinlogActorMessage {
		STOP_CLIENT, START_CLIENT
	}

	public static class InitClient implements SubTableBinlogActorMessage {
		public final ActorRef<SubTableBinlogActorMessage> ackRef;
		public final String fromOffset;

		public InitClient(ActorRef<SubTableBinlogActorMessage> ackRef, String fromOffset) {
			this.ackRef = ackRef;
			this.fromOffset = fromOffset;
		}
	}

}
