package com.Rezar.dbSub.base.event;

import org.apache.commons.lang3.tuple.Pair;

import com.Rezar.dbSub.utils.CborSerializable;

import akka.actor.typed.ActorRef;
import lombok.Data;
import lombok.ToString;

public interface BinlogServerEvent extends CborSerializable {

	public static interface SingleDbActorMessage extends BinlogServerEvent {

		// 供subTableBinlogActor使用的消息
		public static interface SubTableBinlogActorMessage extends SingleDbActorMessage {

			@Data
			public static class BinlogEventAckMessage implements SubTableBinlogActorMessage {
				public String seqId;

				public BinlogEventAckMessage() {
				}

				public BinlogEventAckMessage(String seqId) {
					this.seqId = seqId;
				}
			}

			public static class SyncBinlogMessageAdaptor implements SubTableBinlogActorMessage {
				public final SyncBinlogMessage msg;

				public SyncBinlogMessageAdaptor(SyncBinlogMessage msg) {
					super();
					this.msg = msg;
				}
			}

		}

		/**
		 * 请求binlog事件请求的响应
		 * 
		 * @say little Boy, don't be sad.
		 * @name Rezar
		 * @time May 24, 2020 12:52:08 PM
		 * @Desc 些年若许,不负芳华.
		 *
		 */
		public static class SyncBinlogRespMessage implements SingleDbActorMessage {
			public final boolean ok;
			public final String msg;
			public final String durableId;
			public final String fromOffset;
			public final ActorRef<SubTableBinlogActorMessage> replyTo;

			public SyncBinlogRespMessage(boolean ok, String msg, String durableId, String fromOffset,
					ActorRef<SubTableBinlogActorMessage> replyTo) {
				super();
				this.ok = ok;
				this.msg = msg;
				this.durableId = durableId;
				this.fromOffset = fromOffset;
				this.replyTo = replyTo;
			}

			public static SyncBinlogRespMessage justResp(boolean ok, String msg) {
				return new SyncBinlogRespMessage(ok, msg, null, null, null);
			}

			public static SyncBinlogRespMessage justResp(String durableId, String tryToBeginOffset,
					ActorRef<SubTableBinlogActorMessage> clientConnectActor) {
				return new SyncBinlogRespMessage(true, "", durableId, tryToBeginOffset, clientConnectActor);
			}

		}

		// 客户端同步binlog事件的请求
		@ToString
		public static class SyncBinlogMessage implements SingleDbActorMessage {
			public final String dbInstanceName;
			public final String durableId;
			public final String db;
			public final String tableName;
			public final String offset;
			public final ActorRef<SyncBinlogRespMessage> reqReplyTo; // 建立同步请求响应的地址
			public final ActorRef<SubTableBinlogActorMessage> clientActor; // 实际接收binlog事件的客户端地址

			public SyncBinlogMessage(String dbInstanceName, String durableId, String db, String tableName,
					String offset, ActorRef<SyncBinlogRespMessage> reqReplyTo,
					ActorRef<SubTableBinlogActorMessage> clientActor) {
				super();
				this.dbInstanceName = dbInstanceName;
				this.durableId = durableId;
				this.db = db;
				this.tableName = tableName;
				this.offset = offset;
				this.reqReplyTo = reqReplyTo;
				this.clientActor = clientActor;
			}
		}

		public static enum TEST_MSG implements SingleDbActorMessage {
			INSTANCE
		}

		public static enum DB_CONTROLLER implements SingleDbActorMessage {
			INSTANCE
		}

	}

	public static interface BaseActorMessage extends BinlogServerEvent {

		public static class ClientActorDown implements BaseActorMessage {
			public final ActorRef<BinlogRegisterClientResp> watchActor;

			public ClientActorDown(ActorRef<BinlogRegisterClientResp> watchActor) {
				super();
				this.watchActor = watchActor;
			}

		}

		/**
		 * 客户端注册自身的消息请求
		 */
		@Data
		public static class BinlogRegisterClientReq implements BaseActorMessage {
			public final String username;
			public final String password;
			public final ActorRef<BinlogRegisterClientResp> replyTo;

			public BinlogRegisterClientReq(String username, String password,
					ActorRef<BinlogRegisterClientResp> replyTo) {
				super();
				this.username = username;
				this.password = password;
				this.replyTo = replyTo;
			}
		}

		public static class BinlogRegisterClientResp implements BaseActorMessage {

			public static final BinlogRegisterClientResp FAILURE = new BinlogRegisterClientResp(false, "凭证无效");
			public static final BinlogRegisterClientResp SUCCESS = new BinlogRegisterClientResp(true, "^_^");

			public final boolean registerOk;
			public final String msg;

			public BinlogRegisterClientResp(boolean registerOk, String msg) {
				super();
				this.registerOk = registerOk;
				this.msg = msg;
			}

			public static final BinlogRegisterClientResp of(Pair<Boolean, String> res) {
				return new BinlogRegisterClientResp(res.getKey(), res.getValue());
			}
		}
	}

}
