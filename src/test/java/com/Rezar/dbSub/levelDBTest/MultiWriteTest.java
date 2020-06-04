package com.Rezar.dbSub.levelDBTest;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;

import com.Rezar.dbSub.base.ServerConstants;
import com.Rezar.dbSub.utils.GU;

import lombok.extern.slf4j.Slf4j;

/**
 * 多写看是否能提高速度
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time Jun 3, 2020 3:43:39 PM
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public class MultiWriteTest implements Runnable {

	private volatile DB db;
	private File dir;

	private volatile boolean stop;

	private Object replaceLock = new Object();

	private ScheduledExecutorService sch = Executors.newScheduledThreadPool(1);

	public static void main(String[] args) throws IOException, InterruptedException {
		ServerConstants.MAX_STORAGE_EVENT_COUNT = 5000;
		new MultiWriteTest();
	}

	public MultiWriteTest() throws IOException {
		Pair<DB, File> ret = initDB(true);
		this.db = ret.getKey();
		this.dir = ret.getRight();
		CompletableFuture<Void> runAsync = CompletableFuture.runAsync(() -> {
			while (!stop) {
				try {
					for (int c = 0; c < 100000; c++) {
						String key = "0_" + c + "_" + 0;
						synchronized (replaceLock) {
							this.db.put(key.getBytes(), RandomStringUtils.random(20).getBytes());
						}
					}
					log.info("after write dir totalSize:{}", getDirSize(dir));
					TimeUnit.SECONDS.sleep(RandomUtils.nextInt(60, 80));
				} catch (InterruptedException e) {
					e.printStackTrace();
					break;
				}
			}
		});
		ScheduledFuture<?> scheduleWithFixedDelay = sch.scheduleWithFixedDelay(this, 0, 30, TimeUnit.SECONDS);

		Scanner sc = new Scanner(System.in);
		sc.nextLine();
		sc.close();
		runAsync.cancel(true);
		scheduleWithFixedDelay.cancel(true);
		this.sch.shutdownNow();
	}

	private static Pair<DB, File> initDB(boolean reverse, File... dir) throws IOException {
		Options options = new Options();
		options.createIfMissing(true);
		options.comparator(ServerConstants.initComparator(reverse));
		File file = GU.isNullOrEmpty(dir) ? new File("/Users/rezar/Desktop/", UUID.randomUUID().toString()) : dir[0];
		log.info("db dir:{}", file.getAbsolutePath());
		DB open = factory.open(file, options);
		return Pair.of(open, file);
	}

	@Override
	public void run() {
		ReadOptions readOpt = new ReadOptions();
		readOpt.fillCache(false);
		DBIterator iterator = this.db.iterator(readOpt);
		iterator.seekToFirst();
		long curCount = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), true).count();
		if (curCount >= ServerConstants.MAX_STORAGE_EVENT_COUNT) {
			long delCount = curCount - ServerConstants.MAX_STORAGE_EVENT_COUNT;
			iterator.seekToFirst();
			byte[] firstKey = null;
			byte[] lastKey = null;
			while (iterator.hasNext() && delCount-- > 0) {
				Entry<byte[], byte[]> next = iterator.next();
				if (firstKey == null) {
					firstKey = next.getKey();
				}
				lastKey = next.getKey();
				this.db.delete(next.getKey());
			}
			this.db.compactRange(firstKey, lastKey);
			synchronized (replaceLock) {
				try {
					this.db.close();
					Pair<DB, File> initDB = initDB(false, this.dir);
					this.db = initDB.getKey();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			log.info("pre:{} delCount:{} after del ,dir totalSize:{}", curCount, delCount, getDirSize(dir));
		}
	}

	public static double getDirSize(File file) {
		// 判断文件是否存在
		if (file.exists()) {
			// 如果是目录则递归计算其内容的总大小
			if (file.isDirectory()) {
				File[] children = file.listFiles();
				double size = 0;
				for (File f : children)
					size += getDirSize(f);
				return size;
			} else {// 如果是文件则直接返回其大小,以“兆”为单位
				double size = (double) file.length() / 1024 / 1024;
				return size;
			}
		} else {
			System.out.println("文件或者文件夹不存在，请检查路径是否正确！");
			return 0.0;
		}
	}

}
