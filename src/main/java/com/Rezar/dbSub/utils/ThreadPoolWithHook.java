package com.Rezar.dbSub.utils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPoolWithHook {

	public static ThreadPoolWithHook POOL = new ThreadPoolWithHook();

	private ThreadPoolWithHook() {
	}

	private ExecutorService executors = Executors.newCachedThreadPool();

	public CompletableFuture<Void> runTask(Runnable task) {
		return CompletableFuture.runAsync(task, executors);
	}

	public void shutdown() {
		executors.shutdown();
	}

}
