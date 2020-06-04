package com.Rezar.dbSub.server.akkaSystem;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import com.Rezar.dbSub.base.enums.SystemServiceKey;
import com.Rezar.dbSub.base.event.BinlogServerEvent;
import com.Rezar.dbSub.base.event.BinlogServerEvent.BaseActorMessage.BinlogRegisterClientReq;
import com.Rezar.dbSub.base.event.BinlogServerEvent.BaseActorMessage.BinlogRegisterClientResp;
import com.Rezar.dbSub.base.event.BinlogServerEvent.BaseActorMessage.ClientActorDown;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.DB_CONTROLLER;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SyncBinlogMessage;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SyncBinlogRespMessage;
import com.Rezar.dbSub.base.identityCheck.IdentityChecker;
import com.Rezar.dbSub.base.identityCheck.IdentityCheckerJustLocal;
import com.Rezar.dbSub.server.dbInfo.DBInfoResolver;
import com.Rezar.dbSub.server.dbInfo.DatabaseInitInfo;

import akka.actor.Address;
import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import lombok.extern.slf4j.Slf4j;

/**
 * 1) 处理初始化多db实例binlog任务Actor的入口actor <br/>
 * 2) 处理客户端链接和消息转发给指定的db+table处理actor <br/>
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 12, 2020 2:21:25 PM
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public final class BinglogBaseActor extends AbstractBehavior<BinlogServerEvent> {

	private final Map<String, ActorRef<SingleDbActorMessage>> dbInstanceActorRefMap = new HashMap<>();
	private final IdentityChecker identityChecker;
	private final Set<Address> validClient = new HashSet<>();

	/**
	 * 根据多个db初始化信息创建多个actor
	 * 
	 * @param context
	 * @param databaseInitInfo
	 */
	private BinglogBaseActor(ActorContext<BinlogServerEvent> context, List<DatabaseInitInfo> databaseInitInfo) {
		super(context);
		this.initService(databaseInitInfo);
		this.identityChecker = new IdentityCheckerJustLocal();
	}

	private void initService(List<DatabaseInitInfo> databaseInitInfo) {
		log.info("begin to init server with initInfos:{}", databaseInitInfo);
		// 开始初始化单个数据库实例的binlog的actor服务
		DBInfoResolver.doInit(databaseInitInfo).forEach(dbInstanceInfo -> {
			String dbIns = dbInstanceInfo.getDatabaseInitInfo().getDbInstanceName();
			// 启动单数据库实例的binlog订阅
			ActorRef<SingleDbActorMessage> singleDbInstanceActorRef = getContext()
					.spawn(SingleDbBinglogActor.create(dbInstanceInfo), "SingleDbActor-" + dbIns);
			// 服务注册
			ServiceKey<SingleDbActorMessage> dbInsServiceKey = SystemServiceKey.dbInsServiceKey(dbIns);
			log.info("dbInsServiceKey is:{}", dbInsServiceKey);
			// 服务发布
			super.getContext().getSystem().receptionist()
					.tell(Receptionist.register(dbInsServiceKey, singleDbInstanceActorRef));
			dbInstanceActorRefMap.put(dbInstanceInfo.getDatabaseInitInfo().getDbInstanceName(),
					singleDbInstanceActorRef);
		});
	}

	public static Behavior<BinlogServerEvent> create(List<DatabaseInitInfo> databaseInitInfo) {
		return Behaviors.setup(context -> new BinglogBaseActor(context, databaseInitInfo));
	}

	/**
	 * 消息处理
	 */
	@Override
	public Receive<BinlogServerEvent> createReceive() {
		return newReceiveBuilder().onMessageEquals(DB_CONTROLLER.INSTANCE, () -> {
			// 向分数据库实例的binlog订阅服务发送停止订阅事件
			dbInstanceActorRefMap.values().parallelStream().forEach(ref -> {
				ref.tell(DB_CONTROLLER.INSTANCE);
			});
			return this;
		}).onMessage(SyncBinlogMessage.class, this::handleSyncBinlogMessage)
				.onMessage(BinlogRegisterClientReq.class, this::processClientRegister)
				.onMessage(ClientActorDown.class, t -> {
					log.info("客户端:{} 下线,移除已注册记录", t.watchActor.path());
					this.validClient.remove(t.watchActor.path().address());
					return this;
				}).build();
	}

	/**
	 * 处理db实例上的binlog订阅事件
	 * 
	 * @param msg
	 * @return
	 */
	private Behavior<BinlogServerEvent> handleSyncBinlogMessage(SyncBinlogMessage msg) {
		boolean validRegisterBefore = this.validRegisterBefore(msg.reqReplyTo.path().address());
		if (!validRegisterBefore) {
			msg.reqReplyTo.tell(SyncBinlogRespMessage.justResp(false, "非法访问"));
		} else if (this.dbInstanceActorRefMap.containsKey(msg.dbInstanceName)) {
			this.dbInstanceActorRefMap.get(msg.dbInstanceName).tell(msg);
		}
		return Behaviors.same();
	}

	private boolean validRegisterBefore(Address address) {
		return this.validClient.contains(address);
	}

	/**
	 * 处理客户端注册 <br/>
	 * 
	 * 流程:<br/>
	 * 1) 验证客户端凭证是否有效 <br/>
	 * 2) 监控客户端actor的生命周期 <br/>
	 * 3) 在子客户端断开连接之后,从可通信下游列表中移除 <br/>
	 * 
	 * @param command
	 * @return
	 */
	private Behavior<BinlogServerEvent> processClientRegister(BinlogRegisterClientReq command) {
		log.info("BinlogRegisterClientReq:{}", command);
		String host = command.replyTo.path().address().host().get();
		Pair<Boolean, String> valid = this.identityChecker.valid(host, command.username, command.password);
		if (!valid.getLeft()) {
			log.info("failure to valid register request from :{} with user-psd:{}:{}", host, command.username,
					command.password);
		}
		this.validClient.add(command.replyTo.path().address());
		// 添加生命周期监控
		super.getContext().watchWith(command.replyTo, new ClientActorDown(command.replyTo));
		command.replyTo.tell(BinlogRegisterClientResp.of(valid));
		return this;
	}
}
