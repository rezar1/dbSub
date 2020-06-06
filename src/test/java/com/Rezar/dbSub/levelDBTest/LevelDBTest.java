package com.Rezar.dbSub.levelDBTest;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBComparator;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;

import com.Rezar.dbSub.base.ServerConstants;
import com.Rezar.dbSub.base.event.SyncEvent;
import com.Rezar.dbSub.utils.JacksonUtil;
import com.Rezar.dbSub.utils.ProtoBufSerializeUtils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LevelDBTest {

	public static void main(String[] args) throws IOException, InterruptedException {
		testCloseAndIteartor();
//		testIteratorForIdeaExtAudit();
//		testIteratorAfterClose();
//		testIteratorPrev();
//		testGetSmaeKey();
//		testSeek();
//		testIterator();
//		testDestory();
	}

	static volatile DB db;

	public static void testCloseAndIteartor() throws IOException, InterruptedException {
		Options options = new Options();
		options.createIfMissing(true);
		options.comparator(new DBComparator() {

			@Override
			public int compare(byte[] o1, byte[] o2) {
				return new String(o1).compareTo(new String(o2));
			}

			@Override
			public String name() {
				return "Rezar";
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
		AtomicInteger count = new AtomicInteger(0);
		File file = new File(FileUtils.getTempDirectory(), UUID.randomUUID().toString());
		db = factory.open(file, options);
		new Thread(() -> {
			while (true) {
				try {
					for (int i = 0; i < 10000; i++) {
						synchronized (db) {
							db.put("1".getBytes(), "1".getBytes());
							count.incrementAndGet();
						}
					}
					TimeUnit.SECONDS.sleep(5);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, "WRITE_THREAD").start();
		new Thread(() -> {
			while (true) {
				try {
					DBIterator iterator = db.iterator();
					iterator.seekToFirst();
					int delCount = count.get() / 2;
					while (iterator.hasNext() && delCount-- > 0) {
						db.delete(iterator.next().getKey());
						count.decrementAndGet();
					}
					synchronized (db) {
						db.close();
						db = factory.open(file, options);
					}
					TimeUnit.SECONDS.sleep(4);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, "CLOSE_THREAD").start();
		new Thread(() -> {
			while (true) {
				try {
					DBIterator iterator = null;
					synchronized (db) {
						iterator = db.iterator();
					}
					if (iterator.hasNext()) {
						iterator.seek(iterator.next().getKey());
					}
					TimeUnit.MILLISECONDS.sleep(70);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}, "ITERATOR_THREAD").start();

		TimeUnit.MINUTES.sleep(10000);

	}

	public static void testIteratorForIdeaExtAudit() throws IOException {
		String paht = "/Users/rezar/RezarWorkSpace/eclipseWorkSpcae2/dbSub/dsp-ad/dsp/idea_ext_audit/1591154739194";
		Options options = new Options();
		options.createIfMissing(true);
		options.comparator(ServerConstants.initComparator());
		DB open = factory.open(new File(paht), options);

		DBIterator iterator = open.iterator();
		iterator.seekToFirst();
		while (iterator.hasNext()) {
			log.info("data:{}", JacksonUtil
					.obj2Str(ProtoBufSerializeUtils.deserialize(iterator.next().getValue(), SyncEvent.class)));
		}
		iterator.close();
		open.close();
	}

	public static void testIteratorAfterClose() throws IOException {
		Options options = new Options();
		options.createIfMissing(true);
		File file = new File(FileUtils.getTempDirectory(), UUID.randomUUID().toString());
		DB db = factory.open(file, options);

		db.put("1".getBytes(), "1".getBytes());
		db.put("2".getBytes(), "1".getBytes());
		db.put("3".getBytes(), "1".getBytes());

		db.close();
		db.iterator();

	}

	public static void testIteratorPrev() throws IOException {

		Options options = new Options();
		options.createIfMissing(true);
		File file = new File(FileUtils.getTempDirectory(), UUID.randomUUID().toString());
		DB db = factory.open(file, options);

		db.put("1".getBytes(), "1".getBytes());
		db.put("2".getBytes(), "1".getBytes());
		db.put("3".getBytes(), "1".getBytes());

		DBIterator iterator = db.iterator();
		iterator.seekToLast();
		String lastKey = iterator.hasNext() ? new String(iterator.next().getKey()) : null;
		log.info("prev:{}", lastKey);
		iterator.seekToLast();
		while (iterator.hasPrev()) {
			log.info("prev:{}", new String(iterator.prev().getKey()));
		}
		iterator.close();
		db.close();
	}

	/**
	 * 测试comparator是否能影响key相同的判断
	 * 
	 * @throws IOException
	 */
	public static void testGetSmaeKey() throws IOException {
		Options options = new Options();
		options.createIfMissing(true);
		options.comparator(ServerConstants.initComparator());
		File file = new File(FileUtils.getTempDirectory(), "testlevel");
		DB db = factory.open(file, options);
		String key1 = "1_2_3";
		String key2 = "1_2_4";
		db.put(key1.getBytes(), "Rezar".getBytes());
		db.put(key2.getBytes(), "Rezar2".getBytes());
		System.out.println(new String(db.get(key1.getBytes())));

		DBIterator iterator = db.iterator();
		iterator.seekToFirst();
		while (iterator.hasNext()) {
			Entry<byte[], byte[]> next = iterator.next();
			log.info("with key:{} and value:{}", new String(next.getKey()), new String(next.getValue()));
		}
		System.exit(-1);
		db.close();
	}

	public static void testSeek() throws IOException {
		Options options = new Options();
		options.createIfMissing(true);
		options.comparator(ServerConstants.initComparator());
		File file = new File(
				"/Users/rezar/RezarWorkSpace/eclipseWorkSpcae2/dbSub/dsp-ad/dsp/idea_ext_audit/1590492760377");
		DB db = factory.open(file, options);
		ReadOptions readOptions = new ReadOptions();
		readOptions.fillCache(false);
		DBIterator iterator = db.iterator(readOptions);
//		iterator.seekToLast();
		iterator.seek("19".getBytes());
		log.info("has next:{}-{}", iterator.hasNext(), new String(iterator.next().getKey()));
//		iterator.close();
//		db.close();
	}

	public static void testDestory() throws IOException {
		Options options = new Options();
		options.createIfMissing(true);
		File file = new File(
				"/Users/rezar/RezarWorkSpace/eclipseWorkSpcae2/dbSub/dsp-ad/dsp/idea_ext_audit/1590492760377");
		factory.destroy(file, options);
	}

	public static void testIterator() throws IOException, InterruptedException {
		// Runtime.getRuntime().exec("ulimit -c unlimited");
		Options options = new Options();
		DBComparator dbComparator = new DBComparator() {
			@Override
			public int compare(byte[] o1, byte[] o2) {
				String key1 = new String(o1);
				String key2 = new String(o2);
				return key1.compareTo(key2);
			}

			@Override
			public String name() {
				return "offsetComparator";
			}

			@Override
			public byte[] findShortestSeparator(byte[] start, byte[] limit) {
				return start;
			}

			@Override
			public byte[] findShortSuccessor(byte[] key) {
				return key;
			}
		};
		options.comparator(dbComparator);
		options.createIfMissing(true);
		File file = new File("/Users/rezar/Desktop/levelDB2/");
		boolean isNew = !file.exists();
		DB db = factory.open(file, options);
		try {
			ReadOptions readOptions = new ReadOptions();
			DBIterator iterator = db.iterator(readOptions);
			int keyIndex = 0;
			for (; keyIndex < 26; keyIndex++) {
				char curChar = (char) ('a' + keyIndex);
				byte[] key = (curChar + "").getBytes();
				byte[] value = new String("value" + keyIndex).getBytes();
				db.put(key, value);
			}
			TimeUnit.SECONDS.sleep(1);
			if (isNew) {
				iterator = db.iterator(readOptions);
			}
			String keyFirst = new String("r");
			iterator.seek(keyFirst.getBytes());
			while (iterator.hasNext()) {
				System.out.println(new String(iterator.next().getKey()));
			}
			iterator.close();
		} finally {
			db.close();
		}
	}

}
