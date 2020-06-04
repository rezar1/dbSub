package com.Rezar.dbSub.levelDBTest;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

public class LevelDBReadAndIteratorAsyncTest2 {

	private DB db;

	public LevelDBReadAndIteratorAsyncTest2(File path) throws IOException, InterruptedException {
		Options options = new Options();
		options.createIfMissing(true);
		options.comparator(new DBComparator() {
			@Override
			public int compare(byte[] o1, byte[] o2) {
				return (int) (Long.parseLong(new String(o1)) - Long.parseLong(new String(o2)));
			}

			@Override
			public String name() {
				return "Number Key Comparator";
			}

			@Override
			public byte[] findShortestSeparator(byte[] start, byte[] limit) {
				return start;
			}

			@Override
			public byte[] findShortSuccessor(byte[] key) {
				return key;
			}
		});
		this.db = factory.open(path, options);
		AtomicBoolean stop = new AtomicBoolean(false);

		Object newDataLock = new Object();

		Thread writeT = new Thread(() -> {
			int index = 0;
			while (!stop.get()) {
				int randomValue = RandomUtils.nextInt(5, 10);
				for (int i = 0; i < randomValue; i++) {
					String indexKey = String.valueOf(index++);
					this.db.put(indexKey.getBytes(), indexKey.getBytes());
					synchronized (newDataLock) {
						newDataLock.notify();
					}
				}
//				try {
//					TimeUnit.SECONDS.sleep(RandomUtils.nextInt(1, 3));
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
			}
		});

		Thread iterT = new Thread(() -> {
			String lastRead = null;
			while (!stop.get()) {
				DBIterator iterator = this.db.iterator();
				if (lastRead != null) {
					iterator.seek(lastRead.getBytes());
					iterator.next();
				} else {
					iterator.seekToFirst();
				}
				while (iterator.hasNext()) {
					lastRead = new String(iterator.next().getKey());
					System.out.println("Slave_Iter:" + lastRead);
					try {
						TimeUnit.MILLISECONDS.sleep(10);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				try {
					iterator.close();
					synchronized (newDataLock) {
						newDataLock.wait();
					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		});

		writeT.start();
		iterT.start();

		Scanner sc = new Scanner(System.in);
		sc.nextLine();
		stop.set(true);

		DBIterator iterator = this.db.iterator();
		iterator.seekToFirst();
		iterator.forEachRemaining(entry -> {
			System.out.println("Main_Iter:" + new String(entry.getKey()));
		});
		sc.close();
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		File file = new File(FileUtils.getTempDirectory(), UUID.randomUUID().toString());
		new LevelDBReadAndIteratorAsyncTest2(file);
		file.delete();
	}

}
