package com.Rezar.dbSub.akkaTest;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import akka.actor.typed.ActorSystem;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.BehaviorBuilder;
import akka.actor.typed.javadsl.Behaviors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AkkaStopTest {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		ActorSystem<String> system = ActorSystem.create(buildBehavior(), "test");
		system.tell("xixi");
//		system.tell("stop");
		TimeUnit.SECONDS.sleep(3);
		system.terminate();
		system.getWhenTerminated().toCompletableFuture().thenAccept(System.out::println);

	}

	private static Behavior<String> buildBehavior() {
		return Behaviors.setup(context -> {
			context.spawn(buildChild("child1"), "child1");
			context.spawn(buildChild("child2"), "child2");
			context.spawn(buildChild("child3"), "child3");
			return BehaviorBuilder.<String>create().onMessageEquals("xixi", () -> {
				log.info("XIXI-----");
				return Behaviors.same();
			}).onMessageEquals("stop", () -> {
				return Behaviors.stopped();
			}).onSignal(PostStop.class, msg -> {
				log.info("Parent PostStop called");
				TimeUnit.SECONDS.sleep(5);
				return Behaviors.same();
			}).build();
		});
	}

	private static Behavior<String> buildChild(String childName) {
		return BehaviorBuilder.<String>create().onMessageEquals("xixi", () -> {
			log.info("CHILD XIXI-----");
			return Behaviors.same();
		}).onMessageEquals("stop", () -> {
			return Behaviors.stopped();
		}).onSignal(PostStop.class, msg -> {
			log.info(childName + " : CHILD PostStop called");
			return Behaviors.same();
		}).build();
	}

}
