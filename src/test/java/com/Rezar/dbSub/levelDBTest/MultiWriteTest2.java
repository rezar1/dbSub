package com.Rezar.dbSub.levelDBTest;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;

import com.Rezar.dbSub.base.ServerConstants;
import com.Rezar.dbSub.utils.GU;
import com.google.common.base.Stopwatch;

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
public class MultiWriteTest2 {

	public static void main(String[] args) throws IOException, InterruptedException {
		testWrite();
	}

	public static void testWrite() throws IOException, InterruptedException {
		ReadOptions options = new ReadOptions();
		options.fillCache(false);
		int writeCount = 200000;
		int storeCount = 100000;
		Pair<DB, File> ret = initDB(false);
		DB open = ret.getKey();
		Stopwatch watch = Stopwatch.createStarted();
		String key = null;
		for (int c = 0; c < writeCount; c++) {
			key = "0_" + c + "_" + 0;
			open.put(key.getBytes(), key.getBytes());
		}
		log.info("key:{}", key);
		watch.stop();
		log.info("write use time:{}", watch.elapsed(TimeUnit.MILLISECONDS));
		open.close();

		Pair<DB, File> dbRet = initDB(true, ret.getValue());
		DB openBak = dbRet.getKey();
		DBIterator iterator = openBak.iterator(options);
		iterator.seekToFirst();
		int count = 0;
		DB db2 = initDB(false).getKey();
		watch = Stopwatch.createStarted();
		File tmpFile1 = new File(FileUtils.getTempDirectory(), UUID.randomUUID().toString() + ".txt");
		PrintWriter pw1 = new PrintWriter(tmpFile1);
		log.info("tmpFile1 write to :{}", tmpFile1.getAbsolutePath());
		while (iterator.hasNext() && count++ < storeCount) {
			Entry<byte[], byte[]> data = iterator.next();
			db2.put(data.getKey(), data.getValue());
			pw1.write(new String(data.getKey()) + "\n");
		}
		pw1.close();
		watch.stop();
		log.info("copy:{} use time:{}", count - 1, watch.elapsed(TimeUnit.MILLISECONDS));

		AtomicInteger cou = new AtomicInteger(0);
		DBIterator iterator2 = db2.iterator(options);
		iterator2.seekToFirst();
		File tmpFile = new File(FileUtils.getTempDirectory(), UUID.randomUUID().toString() + ".txt");
		log.info("res write to :{}", tmpFile.getAbsolutePath());
		PrintWriter pw = new PrintWriter(tmpFile);
		iterator2.forEachRemaining(data -> {
			cou.addAndGet(1);
			tmpKey = new String(data.getKey());
			pw.write(tmpKey + "\n");
		});
		log.info("total record:{} finallyKey:{}", cou.get(), tmpKey);
		pw.close();
		TimeUnit.HOURS.sleep(1);
	}

	private static String tmpKey;

	private static Pair<DB, File> initDB(boolean reverse, File... dir) throws IOException {
		Options options = new Options();
		options.createIfMissing(true);
		options.comparator(ServerConstants.initComparator(reverse));
		File file = GU.isNullOrEmpty(dir) ? new File("/Users/rezar/Desktop/", UUID.randomUUID().toString()) : dir[0];
		log.info("db dir:{}", file.getAbsolutePath());
		DB open = factory.open(file, options);
		return Pair.of(open, file);
	}

}
