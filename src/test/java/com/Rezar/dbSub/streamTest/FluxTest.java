package com.Rezar.dbSub.streamTest;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import com.Rezar.dbSub.utils.CircleQueue;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class FluxTest {

	@Test
	public void arrayDequeTest() {
		CircleQueue<String> deque = new CircleQueue<String>(3);
		deque.add("Rezar1");
		deque.add("Rezar2");
		deque.add("Rezar3");
		deque.add("deque4");
		String take = null;
		while ((take = deque.get()) != null) {
			System.out.println(take);
		}
	}

	@Test
	public void testInfiniteFlux() {
		final Random random = new Random();
		Flux.generate(sink -> {
			sink.next(random.nextInt(100));
		}).subscribe(data -> {
			System.out.println("sinkData:" + data);
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});

//		Flux.generate(ArrayList::new, (list, sink) -> {
//			int value = random.nextInt(100);
//			System.out.println("add value:" + value);
//			list.add(value);
//			sink.next(value);
//			if (list.size() == 10) {
//				sink.complete();
//			}
//			return list;
//		}).subscribe(data -> {
//			System.out.println("data:" + data);
//			try {
//				TimeUnit.SECONDS.sleep(1);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		});

	}

	@Test
	public void infiniteTest() {
		Supplier<String> suppiler = new Supplier<String>() {
			private List<String> cacheData = new ArrayList<>();
			private Iterator<String> curIter = null;
			private int dataCount;

			private void bufferData() {
				cacheData.clear();
				for (int i = 0; i < 5; i++) {
					cacheData.add("str-" + dataCount++);
				}
				curIter = cacheData.iterator();
				System.out.println("new batch data");
			}

			@Override
			public String get() {
				if (curIter == null || !curIter.hasNext()) {
					bufferData();
				}
				return curIter.next();
			}
		};
		Stream.generate(suppiler).forEach(data -> {
			System.out.println("data:" + data);
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		});
	}

	@Test
	public void testFlux() throws InterruptedException {
		System.out.println("curThread of Main:" + Thread.currentThread().getName());
		BlockingQueue<String> blockQueue = new ArrayBlockingQueue<String>(1000);
		Flux.<String>generate(sink -> {
			try {
				String event = blockQueue.take();
				sink.next(event);
				System.out.println("curThread of Sink:" + Thread.currentThread().getName());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}).publishOn(Schedulers.elastic()).subscribeOn(Schedulers.elastic()).subscribe(msg -> {
			System.out.println("curThread of subscribe:" + Thread.currentThread().getName());
			System.out.println(msg);
		});
		final Random random = new Random();
		for (int i = 0; i < 20; i++) {
			TimeUnit.MILLISECONDS.sleep(random.nextInt(1200));
			blockQueue.put("event-" + random.nextInt(100));
		}
		TimeUnit.MINUTES.sleep(5);
	}

	@Test
	public void testFlux2() throws InterruptedException {
		System.out.println("curThread of Main:" + Thread.currentThread().getName());
		BlockingQueue<String> blockQueue = new ArrayBlockingQueue<String>(1000);
		Flux.<String>generate(sink -> {
			try {
				String event = blockQueue.take();
				sink.next(event);
				System.out.println("curThread of Sink:" + Thread.currentThread().getName());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}).publishOn(Schedulers.elastic()).subscribeOn(Schedulers.elastic()).subscribe(msg -> {
			System.out.println("curThread of subscribe:" + Thread.currentThread().getName());
			System.out.println(msg);
		});
		final Random random = new Random();
		for (int i = 0; i < 20; i++) {
			TimeUnit.MILLISECONDS.sleep(random.nextInt(1200));
			blockQueue.put("event-" + random.nextInt(100));
		}
		TimeUnit.MINUTES.sleep(5);
	}

}
