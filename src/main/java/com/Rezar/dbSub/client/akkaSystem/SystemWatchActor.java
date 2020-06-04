package com.Rezar.dbSub.client.akkaSystem;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.Rezar.dbSub.base.event.BinlogServerEvent;
import com.Rezar.dbSub.client.akkaSystem.SystemWatchActor.ClientSystenEvent;
import com.Rezar.dbSub.client.interfaces.BinlogServerStatusListener.OnDbDown;
import com.Rezar.dbSub.client.interfaces.BinlogServerStatusListener.OnDbUp;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;

/**
 * 处理系统事件通知的Actor
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 27, 2020 3:01:41 PM
 * @Desc 些年若许,不负芳华.
 *
 */
public class SystemWatchActor extends AbstractBehavior<ClientSystenEvent> {

	private Set<OnDbUp> onDbUpConsumer = new HashSet<>();
	private Set<OnDbDown> onDbDownConsumer = new HashSet<>();

	public SystemWatchActor(ActorContext<ClientSystenEvent> context, List<OnDbUp> onDbUp, List<OnDbDown> onDbDown) {
		super(context);
		this.onDbUpConsumer.addAll(onDbUp);
		this.onDbDownConsumer.addAll(onDbDown);
	}

	public static Behavior<ClientSystenEvent> create(List<OnDbUp> onDbUp, List<OnDbDown> onDbDown) {
		return Behaviors.<ClientSystenEvent>setup(context -> new SystemWatchActor(context, onDbUp, onDbDown));
	}

	@Override
	public Receive<ClientSystenEvent> createReceive() {
		return super.newReceiveBuilder().onMessage(DbUpEventListenerOpt.class, msg -> {
			if (msg.add) {
				this.onDbUpConsumer.add(msg.onDbUp);
			} else {
				this.onDbUpConsumer.remove(msg.onDbUp);
			}
			return this;
		}).onMessage(DbDownEventListenerOpt.class, msg -> {
			if (msg.add) {
				this.onDbDownConsumer.add(msg.onDbDown);
			} else {
				this.onDbDownConsumer.remove(msg.onDbDown);
			}
			return this;
		}).onMessage(DbRegisteResult.class, msg -> {
			// 单个数据库实例上的库表订阅注册结果
			if (msg.success) {
				this.onDbUpConsumer.parallelStream().forEach(up -> up.onDbUp(msg.dbIns));
			} else {
				this.onDbDownConsumer.parallelStream().forEach(up -> up.onDbDown(msg.dbIns));
			}
			return this;
		}).build();
	}

	public static class DbUpEventListenerOpt implements ClientSystenEvent {
		final OnDbUp onDbUp;
		final boolean add;

		public DbUpEventListenerOpt(OnDbUp onDbUp, boolean add) {
			super();
			this.onDbUp = onDbUp;
			this.add = add;
		}

	}

	public static class DbDownEventListenerOpt implements ClientSystenEvent {
		final OnDbDown onDbDown;
		final boolean add;

		public DbDownEventListenerOpt(OnDbDown onDbDown, boolean add) {
			super();
			this.onDbDown = onDbDown;
			this.add = add;
		}

	}

	public static class DbRegisteResult implements ClientSystenEvent {
		public final String dbIns;
		public final boolean success;

		public DbRegisteResult(String dbIns, boolean success) {
			this.dbIns = dbIns;
			this.success = success;
		}
	}

	public static interface ClientSystenEvent extends BinlogServerEvent {
	}

}
