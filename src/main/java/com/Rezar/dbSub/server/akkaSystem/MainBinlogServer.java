package com.Rezar.dbSub.server.akkaSystem;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PreDestroy;

import org.springframework.stereotype.Service;
import org.xeustechnologies.jcl.JarClassLoader;

import com.Rezar.dbSub.base.event.BinlogServerEvent;
import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage.DB_CONTROLLER;
import com.Rezar.dbSub.server.configParser.ConfigLoader;
import com.Rezar.dbSub.server.dbInfo.DatabaseInitInfo;
import com.Rezar.dbSub.utils.GU;
import com.Rezar.dbSub.utils.ThreadPoolWithHook;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.BehaviorBuilder;
import akka.actor.typed.javadsl.Behaviors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class MainBinlogServer {

	private ActorSystem<BinlogServerEvent> system;
	private List<DatabaseInitInfo> databaseInitInfo;

	private String akkaConfig;
	private boolean fromFile;

	public void init(String akkaConfig, String configPath, boolean fromFile) throws Exception {
		if (fromFile) {
			databaseInitInfo = ConfigLoader.parserConfigFromFile(configPath);
		} else {
			databaseInitInfo = ConfigLoader.parserConfigFromClasspath(configPath);
		}
		this.akkaConfig = akkaConfig;
		this.fromFile = fromFile;
		if (GU.isNullOrEmpty(databaseInitInfo)) {
			log.warn("empty databaseInitInfo,please checkout");
			return;
		}
	}

	/**
	 * 维持服务的单一入口
	 * 
	 * @param databaseInitInfo
	 * @return
	 */
	private static Behavior<BinlogServerEvent> create(List<DatabaseInitInfo> databaseInitInfo) {
		return Behaviors.setup(context -> {
			ActorRef<BinlogServerEvent> spawn = context.spawn(BinglogBaseActor.create(databaseInitInfo),
					"binlogServer");
			return BehaviorBuilder.<BinlogServerEvent>create().onAnyMessage(msg -> {
				spawn.tell(msg);
				return Behaviors.same();
			}).build();
		});
	}

	@PreDestroy
	public void destory() {
		log.info("binlogServer Exit");
		// 发送停机消息
		this.system.tell(DB_CONTROLLER.INSTANCE);
		this.system.terminate();
		this.system.getWhenTerminated().toCompletableFuture().thenAccept(done -> {
			ThreadPoolWithHook.POOL.shutdown();
			log.info("{{{{{{{{{{{{{{{{{{{{{{{ค(TㅅT)再见");
		});
	}

	public void startup() {
		if (GU.isNullOrEmpty(databaseInitInfo)) {
			return;
		}
		Map<String, Object> overrides = new HashMap<>();
		overrides.put("akka.cluster.roles", Collections.singletonList("binlogServer"));
		Config load = null;
		if (fromFile) {
			JarClassLoader jarClassLoader = new JarClassLoader(MainBinlogServer.class.getClassLoader());
			jarClassLoader.add(this.akkaConfig);
			load = ConfigFactory.load(jarClassLoader, new File(this.akkaConfig).getName().replaceAll("\\..*", ""));
		} else {
			load = ConfigFactory.load(this.akkaConfig);
		}
		Config config = ConfigFactory.parseMap(overrides).withFallback(load);
		system = ActorSystem.create(create(databaseInitInfo), "binlogSyncSystem", config);
		log.info("binlogServer startup");

	}
}
