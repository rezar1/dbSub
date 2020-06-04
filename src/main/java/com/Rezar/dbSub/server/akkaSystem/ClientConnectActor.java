package com.Rezar.dbSub.server.akkaSystem;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;

import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SubTableBinlogActorMessage;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SubTableBinlogActorMessage.BinlogEventAckMessage;
import com.Rezar.dbSub.base.event.SyncEvent;
import com.Rezar.dbSub.server.interfaces.BinlogDataStorage;
import com.Rezar.dbSub.server.interfaces.DbInsPositionRecorder;
import com.Rezar.dbSub.server.interfaces.MsgNextIterator;
import com.Rezar.dbSub.utils.ThreadPoolWithHook;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import lombok.extern.slf4j.Slf4j;

/**
 * 1) 推送事件到客户端 <br/>
 * 2) 变更状态以处理慢消费状况
 * 
 * 实现:完全使用mongodb作为消费缓冲区 <br/>
 * 1)开启ClientConnectActor以接收父Actor最新事件推送<br/>
 * 2)开启单独的消费Actor发送事件及接收ack信号<br/>
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 12, 2020 2:21:25 PM
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public final class ClientConnectActor extends AbstractBehavior<SubTableBinlogActorMessage> {

	private final byte[] durableId;
	public final ActorRef<SubTableBinlogActorMessage> clientActorRef; // 客户端通信Actor
	private DbInsPositionRecorder positionRecorder;
	private final LevelDBReader dbReader;
	private final BinlogDataStorage dataStorage;
	private CompletableFuture<Void> runTask;

	// 用于控制发送给下游消息的数量,避免大数据量导致的下游OOM
	private final Semaphore sendSema = new Semaphore(3000);

	private ClientConnectActor(ActorContext<SubTableBinlogActorMessage> context,
			ActorRef<SubTableBinlogActorMessage> clientActorRef, String fromOffset, String durableId,
			DbInsPositionRecorder posRecorder, BinlogDataStorage dataStorage) {
		super(context);
		this.durableId = durableId.getBytes();
		this.clientActorRef = clientActorRef;
		this.positionRecorder = posRecorder;
		this.dataStorage = dataStorage;
		dbReader = new LevelDBReader(fromOffset);
		super.getContext().watch(this.clientActorRef);
	}

	public static Behavior<SubTableBinlogActorMessage> create(ActorRef<SubTableBinlogActorMessage> clientActorRef,
			String tryToBeginOffset, String durableId, DbInsPositionRecorder posRecorder,
			BinlogDataStorage dataStorage) {
		return Behaviors.<SubTableBinlogActorMessage>setup(context -> new ClientConnectActor(context, clientActorRef,
				tryToBeginOffset, durableId, posRecorder, dataStorage));
	}

	/**
	 * 实现流程:<br/>
	 * 1)如果有历史待消费数据,
	 */
	@Override
	public Receive<SubTableBinlogActorMessage> createReceive() {
		return newReceiveBuilder().onMessage(Start.class, msg -> {
			log.info("try to start ClientConnectActor with path:{} and durId:{}",
					this.clientActorRef.path().address().toString(), new String(this.durableId));
			runTask = ThreadPoolWithHook.POOL.runTask(dbReader);
			return Behaviors.same();
		}).onSignal(PostStop.class, msg -> {
			this.stop();
			return Behaviors.same();
		}).onMessage(Stop.class, msg -> {
			log.info("try to stop ClientConnectActor with path:{} and durId:{}",
					this.clientActorRef.path().address().toString(), new String(this.durableId));
			this.stop();
			return Behaviors.stopped();
		}).onMessage(SyncEvent.class, msg -> {
			return Behaviors.same();
		}).onMessage(BinlogEventAckMessage.class, msg -> {
			// 更新最新的读取offset
			this.positionRecorder.storeClientCurrentPos(durableId, msg.seqId.toString());
			// 增加可发送消息信号量
			this.sendSema.release();
			return Behaviors.same();
		}).onSignal(Terminated.class, ter -> {
			log.info("client actor terminated with path:{} and durId:{}",
					this.clientActorRef.path().address().toString(), new String(this.durableId));
			this.stop();
			return Behaviors.stopped();
		}).build();
	}

	private void stop() {
		this.dbReader.shutdown();
		this.runTask.cancel(false);
	}

	public static enum Start implements SubTableBinlogActorMessage {
		INSTANCE
	}

	public static enum Stop implements SubTableBinlogActorMessage {
		INSTANCE
	}

	private class LevelDBReader implements Runnable {

		MsgNextIterator iterator;

		private volatile boolean stop;

		public LevelDBReader(String fromSeqId) {
			iterator = dataStorage.iteratorFrom(fromSeqId);
		}

		@Override
		public void run() {
			try {
				while (!this.stop) {
					SyncEvent event = null;
					while (!this.stop && (event = iterator.blockingTake()) != null) {
						this.sendAndWaitAck(event);
					}
				}
			} catch (Exception ex) {
				log.error("error while run:{}", ex);
			}
		}

		/**
		 * 发送并等待客户端ack回应
		 * 
		 * @param deserialize
		 * @throws InterruptedException
		 */
		private void sendAndWaitAck(SyncEvent deserialize) throws InterruptedException {
			sendSema.acquire();
			clientActorRef.tell(deserialize);
		}

		void shutdown() {
			if (this.stop) {
				return;
			}
			this.stop = true;
			if (iterator != null) {
				iterator.close();
			}
			log.info("LevelDBReader exit for durableId:{}", new String(durableId));
		}
	}

}
