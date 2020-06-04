package com.Rezar.dbSub.client.akkaSystem;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.Rezar.dbSub.client.akkaSystem.SystemWatchActor.ClientSystenEvent;
import com.Rezar.dbSub.client.config.SingleDbInsClientConfigInfo;
import com.Rezar.dbSub.client.interfaces.BinlogServerStatusListener.OnDbDown;
import com.Rezar.dbSub.client.interfaces.BinlogServerStatusListener.OnDbUp;
import com.Rezar.dbSub.utils.GU;
import com.Rezar.dbSub.utils.ServerIpUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.BehaviorBuilder;
import akka.actor.typed.javadsl.Behaviors;
import lombok.extern.slf4j.Slf4j;

/**
 * 客户端Actor初始化入口
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 23, 2020 11:28:03 AM
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public class MainClientBinlogService {

	private List<SingleDbInsClientConfigInfo> allDbInsInitConfig = new ArrayList<SingleDbInsClientConfigInfo>();
	private String binlogAkkaConfig;
	// TODO 添加服务状态变更监听的事件处理
	private List<OnDbUp> onDbUp;
	private List<OnDbDown> onDbDown;

	private ActorSystem<ClientSystenEvent> akkaSystem;

	public MainClientBinlogService(List<SingleDbInsClientConfigInfo> allSingleDbClientConfigInfo, List<OnDbUp> onDbUp,
			List<OnDbDown> onDbDown) {
		this("binlogAkkaClient", allSingleDbClientConfigInfo, onDbUp, onDbDown);
	}

	public MainClientBinlogService(String binlogAkkaConfig,
			List<SingleDbInsClientConfigInfo> allSingleDbClientConfigInfo, List<OnDbUp> onDbUp,
			List<OnDbDown> onDbDown) {
		this.binlogAkkaConfig = GU.isNullOrEmpty(binlogAkkaConfig) ? "binlogAkkaClient" : binlogAkkaConfig;
		this.allDbInsInitConfig = allSingleDbClientConfigInfo;
		this.onDbUp = onDbUp;
		this.onDbDown = onDbDown;
		log.info("onDbUp:{} onDbDown:{}", this.onDbUp.size(), this.onDbDown.size());
	}

	/**
	 * 还是使用集群路由吧,监听集群状态并获取指定ServiceKey的服务
	 * 
	 * 1) 初始化所有数据库实例的客户端
	 * 
	 * 2) 添加远端掉线或者未正常连接上远端的异常监控和通知告警
	 * 
	 * @param databaseInitInfo
	 * @return
	 */
	private Behavior<ClientSystenEvent> create() {
		return Behaviors.setup(context -> {
			// 初始化监控
			ActorRef<ClientSystenEvent> systemWatcher = context
					.spawn(SystemWatchActor.create(this.onDbUp, this.onDbDown), "binlogClientSystemWatcher");
			this.allDbInsInitConfig.parallelStream().forEach(config -> {
				context.spawn(SingleDBInsBinlogClientActor.create(config, systemWatcher),
						"SingleDBInsBinlogClientActor:" + config.getDbIns());
			});
			return BehaviorBuilder.<ClientSystenEvent>create().onAnyMessage(msg -> {
				systemWatcher.tell(msg);
				return Behaviors.same();
			}).build();
		});
	}

	public void startup() {
		Map<String, Object> overrides = new HashMap<>();
		overrides.put("akka.remote.artery.canonical.hostname", ServerIpUtil.SERVER_IP);
		overrides.put("akka.cluster.roles", Collections.singletonList("binlogClient"));
		Config config = ConfigFactory.parseMap(overrides).withFallback(ConfigFactory.load(binlogAkkaConfig));
		akkaSystem = ActorSystem.create(create(), "binlogSyncSystem", config);
		log.info("binlogClient startup");
	}

	public void stop() {
		this.akkaSystem.terminate();
	}

}
