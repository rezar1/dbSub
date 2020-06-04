package com.Rezar.dbSub.client.akkaSystem;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;

import com.Rezar.dbSub.base.enums.SystemServiceKey;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SubTableBinlogActorMessage;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SyncBinlogMessage;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.SyncBinlogRespMessage;
import com.Rezar.dbSub.base.exceptions.StopException;
import com.Rezar.dbSub.client.akkaSystem.SingleTableBinlogClientActor.ClientController;
import com.Rezar.dbSub.client.akkaSystem.SingleTableBinlogClientActor.InitClient;
import com.Rezar.dbSub.client.akkaSystem.SystemWatchActor.ClientSystenEvent;
import com.Rezar.dbSub.client.akkaSystem.SystemWatchActor.DbRegisteResult;
import com.Rezar.dbSub.client.config.SingleDbInsClientConfigInfo;
import com.Rezar.dbSub.client.config.TableBinlogProcessor;
import com.Rezar.dbSub.client.config.TableInitKey;
import com.Rezar.dbSub.client.infoPersistent.ClientDataStoreInfo;
import com.Rezar.dbSub.client.interfaces.OffsetRecorder;
import com.Rezar.dbSub.utils.GU;
import com.Rezar.dbSub.utils.JacksonUtil;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import lombok.extern.slf4j.Slf4j;

/**
 * 监听单数据库实例binlog变化的客户端
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 23, 2020 2:35:18 PM
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public class SingleDBInsBinlogClientActor extends AbstractBehavior<SingleDbActorMessage> {

	private final SingleDbInsClientConfigInfo clientConfigInfo;

	// 所有在线的服务
	private List<ActorRef<SingleDbActorMessage>> allServer = new ArrayList<>();
	// 当前连接的远程服务
	private ActorRef<SingleDbActorMessage> curRemoteServer;
	// 当前数据库实例上服务变更的次数(即远程服务A下线后切换服务到服务B,C,D...的次数)
	private int serverChangeTimes;
	// 所有的订阅表
	private List<SingleTableSyncInfo> allTableSyncInfo;
	// 客户端表处理事件的下标记录表(客户端的业务最好是幂等的)
	private OffsetRecorder offsetRecorder;
	// 接收服务端建立binlog事件通道处理结果的通知地址
	private ActorRef<SyncBinlogRespMessage> registerRespRef;
	// 系统监控
	private ActorRef<ClientSystenEvent> systemWatcher;

	public SingleDBInsBinlogClientActor(ActorContext<SingleDbActorMessage> context,
			SingleDbInsClientConfigInfo clientConfigInfo, ActorRef<ClientSystenEvent> systemWatcher) {
		super(context);
		this.clientConfigInfo = clientConfigInfo;
		this.systemWatcher = systemWatcher;
		log.info("clientConfigInfo are:{}", clientConfigInfo);
		this.offsetRecorder = ClientDataStoreInfo.STORE.initDbOffsetRecorder();
		initTableActors();
		this.registerRespRef = context.messageAdapter(SyncBinlogRespMessage.class,
				ackResp -> new RegisterResp(ackResp));
		assert allTableSyncInfo != null && !allTableSyncInfo.isEmpty();
		// 获取订阅数据库实例的注册服务key
		ServiceKey<SingleDbActorMessage> dbInsServiceKey = SystemServiceKey
				.dbInsServiceKey(clientConfigInfo.getDbIns());
		log.info("dbInsServiceKey is:{}", dbInsServiceKey);
		// 适配服务类型
		ActorRef<Receptionist.Listing> subscriptionAdapter = context.messageAdapter(Receptionist.Listing.class,
				listing -> new ServerUpdate(listing.getServiceInstances(dbInsServiceKey)));
		// 订阅注册服务变更事件
		context.getSystem().receptionist().tell(Receptionist.subscribe(dbInsServiceKey, subscriptionAdapter));
	}

	/**
	 * 初始化单table的任务Actor
	 */
	private void initTableActors() {
		this.allTableSyncInfo = clientConfigInfo.getTableBinlogProcessorMap().entrySet().stream()
				.map(entry -> this.initSingleTableActor(entry.getKey(), entry.getValue())).collect(Collectors.toList());
	}

	private SingleTableSyncInfo initSingleTableActor(TableInitKey key, List<TableBinlogProcessor> allProcessor) {
		ActorRef<SubTableBinlogActorMessage> tableActor = super.getContext().spawn(
				SingleTableBinlogClientActor.create(allProcessor, offsetRecorder, key.toTableMark()),
				"SingleTableActor:" + key.getDurableId()); // , MailboxSelector.fromConfig("client.bounded-mailbox")
		return new SingleTableSyncInfo(key, tableActor);
	}

	public static Behavior<SingleDbActorMessage> create(SingleDbInsClientConfigInfo clientConfigInfo,
			ActorRef<ClientSystenEvent> systemWatcher) {
		return Behaviors.setup(context -> new SingleDBInsBinlogClientActor(context, clientConfigInfo, systemWatcher));
	}

	@Override
	public Receive<SingleDbActorMessage> createReceive() {
		return newReceiveBuilder().onMessage(ServerUpdate.class, msg -> {
			if (msg.serviceInstances.isEmpty()) {
				if (this.curRemoteServer == null) {
					log.info("{} first start ,can not find any remote server online", clientConfigInfo.getDbIns());
				} else {
					log.info("{} all services down,offline will be notified", clientConfigInfo.getDbIns());
					this.systemWatcher.tell(new DbRegisteResult(clientConfigInfo.getDbIns(), false));
				}
				return this;
			}
			this.allServer.clear();
			this.allServer.addAll(msg.serviceInstances);
			this.onRemoteServerChagne();
			return this;
		}).onMessage(RegisterResp.class, registerResp -> {
			// 处理注册结果通知
			if (registerResp.ok) {
				this.allTableSyncInfo.parallelStream()
						.filter(info -> info.tableKey.getDurableId().contentEquals(registerResp.durableId)).findAny()
						.ifPresent(info -> {
							registeFailureTable.remove(info.tableKey);
							info.initWithNewRemote(registerResp.connectActor, registerResp.startOffset);
							this.semaphore.release();
						});
			} else {
				// 异常处理
				if (registerWatcher != null && !this.registerWatcher.isCancelled()) {
					log.error("register resp msg:{}", registerResp.msg);
					this.registerWatcher.cancel(true);
				}
			}
			return this;
		}).onSignal(PostStop.class, stop -> {
			this.offsetRecorder.close();
			return this;
		}).build();
	}

	private void onRemoteServerChagne() {
		if (this.curRemoteServer == null || !allServer.contains(curRemoteServer)) {
			log.info("remote server has change,register itSelf:{}", this.curRemoteServer);
			this.curRemoteServer = this.allServer.get(RandomUtils.nextInt(0, this.allServer.size()));
			this.initDbConnectionWatch();
			register();
		}
	}

	private CopyOnWriteArrayList<TableInitKey> registeFailureTable = new CopyOnWriteArrayList<>();
	private volatile Semaphore semaphore = null;
	private volatile CompletableFuture<Boolean> registerWatcher;

	/**
	 * 初始化监控项,当所有表都连接上服务器后,回调系统监听
	 */
	private void initDbConnectionWatch() {
		if (this.registerWatcher != null && !this.registerWatcher.isCancelled()) {
			this.registerWatcher.completeExceptionally(StopException.JUST_OBJ);
		}
		this.semaphore = new Semaphore(this.allTableSyncInfo.size());
		semaphore.drainPermits();
		registeFailureTable.clear();
		registeFailureTable
				.addAll(this.allTableSyncInfo.stream().map(info -> info.tableKey).collect(Collectors.toList()));
		this.registerWatcher = CompletableFuture.<Boolean>supplyAsync(() -> {
			try {
				return this.semaphore.tryAcquire(this.allTableSyncInfo.size(), 2, TimeUnit.MINUTES);
			} catch (InterruptedException e) {
				log.info("timeout or some interrupted,just exit");
			}
			return false;
		}).whenComplete((registerSuccess, error) -> {
			if (error == StopException.JUST_OBJ) {
				// 被新的取消了,直接返回
				return;
			}
			if (registerSuccess != null && registerSuccess) {
				log.info("binlogClient register to dbIns:{} success , will start all binlog consumer",
						this.clientConfigInfo.getDbIns());
			} else {
				registerSuccess = false;
				log.info(
						"binlogClient try to connnect to dbIns:{} mission failure,please checkout binlog Server was running",
						this.clientConfigInfo.getDbIns());
				log.info("all registe failure are:{}", JacksonUtil.obj2Str(registeFailureTable));
			}
			this.handleRegisteResult(registerSuccess);
		});
	}

	private void handleRegisteResult(boolean success) {
		this.allTableSyncInfo.parallelStream().forEach(info -> {
			if (success) {
				info.startClient();
			} else {
				info.stopClient();
			}
		});
		this.systemWatcher.tell(new DbRegisteResult(this.clientConfigInfo.getDbIns(), success));
	}

	/**
	 * 在线服务变更,重新在新服务上注册当前客户端
	 * 
	 */
	private void register() {
		assert this.curRemoteServer != null;
		this.allTableSyncInfo.forEach(syncInfo -> {
			TableInitKey tableKey = syncInfo.tableKey;
			String takeLastOffset = this.offsetRecorder.takeLastOffset(tableKey.toTableMark());
			// 第一次连接服务器,使用客户配置的下标进行请求
			String clientOffset = tableKey.getOffset() == null ? "" : tableKey.getOffset().trim();
			String fromOffset = clientOffset;
			if (this.serverChangeTimes == 0) {
				switch (clientOffset) {
				case "":
				case "FROM_POS":
					// 从指定FROM_POS
					fromOffset = takeLastOffset;
					break;
				case "LAST_POS":
					// 客户端未记录,则使用LAST_POS尝试使用服务端记录的
					fromOffset = GU.isNullOrEmpty(takeLastOffset) ? "LAST_POS" : takeLastOffset;
					break;
				case "CONTINUE":
					fromOffset = "CONTINUE";
					break;
				default:
					break;
				}
			} else {
				fromOffset = takeLastOffset;
			}
			log.info("serverChangeTimes:{} - {} from offset:{}", serverChangeTimes, tableKey.toTableMark(), fromOffset);
			SyncBinlogMessage msg = new SyncBinlogMessage(tableKey.getDbIns(), tableKey.getDurableId(),
					tableKey.getDb(), tableKey.getTable(), fromOffset, this.registerRespRef, syncInfo.tableActor);
			log.info("msg is:{} and curRemoteServer:{}", msg, this.curRemoteServer);
			this.curRemoteServer.tell(msg);
		});
		serverChangeTimes++;
	}

	public void onNormalStop() {
		IOUtils.closeQuietly(offsetRecorder);
	}

	/**
	 * 在线服务列表更新消息
	 */
	private static class ServerUpdate implements SingleDbActorMessage {
		public final List<ActorRef<SingleDbActorMessage>> serviceInstances;

		public ServerUpdate(Set<ActorRef<SingleDbActorMessage>> set) {
			this.serviceInstances = new ArrayList<>(set);
		}
	}

	static class RegisterResp implements SingleDbActorMessage {
		public final boolean ok;
		public final String msg;
		public final String durableId;
		public final String startOffset;
		public final ActorRef<SubTableBinlogActorMessage> connectActor;

		public RegisterResp(SyncBinlogRespMessage respMsg) {
			this.ok = respMsg.ok;
			this.msg = respMsg.msg;
			this.durableId = respMsg.durableId;
			this.startOffset = respMsg.fromOffset;
			this.connectActor = respMsg.replyTo;
		}

	}

	static class SingleTableSyncInfo {

		final TableInitKey tableKey;
		final ActorRef<SubTableBinlogActorMessage> tableActor;

		SingleTableSyncInfo(TableInitKey tableKey, ActorRef<SubTableBinlogActorMessage> tableActor) {
			this.tableKey = tableKey;
			this.tableActor = tableActor;
		}

		/**
		 * 客户端stop
		 */
		public void stopClient() {
			this.tableActor.tell(ClientController.STOP_CLIENT);
		}

		/**
		 * 客户端start
		 */
		public void startClient() {
			this.tableActor.tell(ClientController.START_CLIENT);
		}

		public void initWithNewRemote(ActorRef<SubTableBinlogActorMessage> connectActor, String fromOffset) {
			this.tableActor.tell(new InitClient(connectActor, fromOffset));
		}

	}

}
