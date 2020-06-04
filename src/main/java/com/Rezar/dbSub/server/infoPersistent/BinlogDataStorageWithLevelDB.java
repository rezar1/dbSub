package com.Rezar.dbSub.server.infoPersistent;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map.Entry;
import java.util.Spliterators;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomUtils;
import org.fusesource.leveldbjni.internal.NativeDB.DBException;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;

import com.Rezar.dbSub.base.ServerConstants;
import com.Rezar.dbSub.base.event.SyncEvent;
import com.Rezar.dbSub.base.exceptions.ServerInitException;
import com.Rezar.dbSub.server.interfaces.BinlogDataStorage;
import com.Rezar.dbSub.server.interfaces.MsgNextIterator;
import com.Rezar.dbSub.utils.DateUtils;
import com.Rezar.dbSub.utils.SyncEventProtoBufSerializer;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 * 每隔3w数据新建一个文件夹,清除之前的文件夹
 * 
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 21, 2020 3:02:07 PM
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public class BinlogDataStorageWithLevelDB implements BinlogDataStorage, Runnable {

	private File storageDir;
	private volatile DB db;
	private Object newMsgLock = new Object();
	private Object replaceDBDirLock = new Object();
	private volatile String curWriteSeqId;
	// 当前的表是第一次订阅的时候,本地也没有前一个写入的seqId,导致curWriteSeqId为null,下游接入
	// 的时候也不知道从哪个地方开始继续同步(未指定fromOffset),所以这里添加一个逻辑
	/**
	 * 如果服务端是第一次订阅某个表,使用beginSeqId记录第一个写入的事件
	 * 
	 * 客户端在接入后发现:
	 * 
	 * 1.curWriteSeqId为null,则在下次被唤醒后从beginSeqId开始订阅
	 * 
	 * 2.curWriteSeqId不为null,则直接使用curWriteSeqId
	 * 
	 */
	private volatile String beginSeqId;

	private static final String dirPathFormat = "/%s/%s/%s";

	private SyncEventProtoBufSerializer syncEventSerializer = new SyncEventProtoBufSerializer();

	private ScheduledFuture<?> scheduleWithFixedDelay;

	private ScheduledExecutorService sch = Executors.newScheduledThreadPool(1, new ThreadFactory() {
		@Override
		public Thread newThread(Runnable r) {
			return new Thread(r, storageDir.getName() + "-数据清理线程");
		}
	});

	@Override
	public void init(String dbIns, String db, String table) {
		this.storageDir = new File(
				ServerConstants.SAVE_BASE_FILE.getAbsolutePath() + String.format(dirPathFormat, dbIns, db, table));
		if (!storageDir.exists()) {
			storageDir.mkdirs();
		}
		this.db = initDbInfo(true);
		scheduleWithFixedDelay = sch.scheduleWithFixedDelay(this, 0, 20, TimeUnit.MINUTES);
		log.info("{}:{} - storage init over", db, table);
	}

	private DB initDbInfo(boolean first) {
		try {
			Options options = new Options();
			options.comparator(ServerConstants.initComparator());
			options.createIfMissing(true);
			DB db = factory.open(storageDir, options);
			if (first) {
				DBIterator iterator = db.iterator();
				iterator.seekToLast();
				curWriteSeqId = iterator.hasNext() ? new String(iterator.next().getKey()) : this.curWriteSeqId;
				IOUtils.closeQuietly(iterator);
				log.info("{} init from offset:{}", this.storageDir.getName(),
						this.curWriteSeqId != null ? new String(this.curWriteSeqId) : "not before");
			}
			return db;
		} catch (DBException ex) {
			throw new ServerInitException("Resource temporarily unavailable:" + storageDir.getAbsolutePath());
		} catch (IOException e) {
			log.error("BinlogDataStorageWithLevelDB init db error:{}", e);
			throw new ServerInitException("unknow exception while initDbInfo", e);
		}
	}

	/**
	 * 存储并分配存储自增id
	 */
	@Override
	public void store(SyncEvent event) {
		if (event != null) {
			if (this.beginSeqId == null) {
				this.beginSeqId = event.getSeqId();
			}
			this.curWriteSeqId = event.getSeqId();
			// log.info("new event:{}", JacksonUtil.obj2Str(event));
			byte[] serialize = this.syncEventSerializer.serialize(event);
			synchronized (replaceDBDirLock) {
				this.db.put(event.getSeqId().getBytes(), serialize);
			}
			synchronized (newMsgLock) {
				newMsgLock.notifyAll();
			}
		}
	}

	@Override
	public MsgNextIterator iteratorFrom(String offset) {
		return new MsgIterator(offset);
	}

	public void close() throws IOException {
		this.scheduleWithFixedDelay.cancel(true);
		this.sch.shutdown();
		this.db.close();
	}

	private class MsgIterator implements MsgNextIterator {

		volatile boolean stop;

		String lastReadOffset;
		DBIterator dbIterator;
		ReadOptions readOptions;

		MsgIterator(String fromOffset) {
			this.readOptions = new ReadOptions();
			this.readOptions.fillCache(false);
			lastReadOffset = fromOffset != null ? fromOffset : curWriteSeqId;
			log.info("MsgIterator for:{} and lastReadOffset:{}", storageDir.getName(),
					lastReadOffset == null ? "not before" : new String(lastReadOffset));
		}

		@Override
		public SyncEvent blockingTake() throws InterruptedException {
			while (!stop) {
				Entry<byte[], byte[]> eventEntry = null;
				if (dbIterator == null) {
					if (this.lastReadOffset != null && curWriteSeqId != null && ServerConstants.COMPARATOR
							.compare(this.lastReadOffset.getBytes(), curWriteSeqId.getBytes()) > 0) {
						// seek到了一个不存在的位置,等待binlog文件同步到当前位置
						// this.lastReadOffset = null;
						// 沉睡
						log.warn("client with path:{} from offset:{} misMatch,try to sleep", storageDir.getName(),
								this.lastReadOffset);
						TimeUnit.MILLISECONDS.sleep(RandomUtils.nextInt(500, 3500));
						continue;
					}
					dbIterator = db.iterator(readOptions);
					if (this.lastReadOffset != null) {
						dbIterator.seek(this.lastReadOffset.getBytes());
						if (dbIterator.hasNext()) {
							dbIterator.next();
						} else {
							// seek到了一个不存在的位置,等待binlog文件同步到当前位置
							// this.lastReadOffset = null;
							// 沉睡
							log.warn(
									"during runing , client with path:{} from offset:{} misMatch(curWriteSeqId:{}),try to sleep",
									storageDir.getName(), this.lastReadOffset, curWriteSeqId);
							TimeUnit.MILLISECONDS.sleep(RandomUtils.nextInt(570, 1500));
							continue;
						}
					} else if (beginSeqId != null) {
						log.info("MsgIterator :{} change lastReadOffset to beginSeqId:{}", storageDir.getName(),
								new String(beginSeqId));
						this.lastReadOffset = beginSeqId;
						dbIterator.seek(this.lastReadOffset.getBytes());
					}
				}
				try {
					if (dbIterator.hasNext() && (eventEntry = dbIterator.next()) != null) {
						this.lastReadOffset = new String(eventEntry.getKey());
						return new SyncEvent(eventEntry.getValue());
					} else {
						if (this.dbIterator != null) {
							IOUtils.closeQuietly(dbIterator);
							this.dbIterator = null;
						}
					}
				} catch (org.iq80.leveldb.DBException e) {
					// 历史的db被清理了,直接循环从新的db开始读
					log.info("{}:old db was closed , try sleep and continue with newDb", storageDir.getName());
					TimeUnit.MILLISECONDS.sleep(RandomUtils.nextInt(10, 30));
					continue;
				}
				synchronized (newMsgLock) {
					if ((this.lastReadOffset == null && curWriteSeqId != null)
							|| (this.lastReadOffset != null && curWriteSeqId != null && ServerConstants.COMPARATOR
									.compare(this.lastReadOffset.getBytes(), curWriteSeqId.getBytes()) < 0)) {
						continue;
					}
					if (this.stop) {
						break;
					}
					newMsgLock.wait();
				}
			}
			return null;
		}

		@Override
		public void close() {
			synchronized (newMsgLock) {
				this.stop = true;
				newMsgLock.notify();
			}
			log.info("MsgIterator exit");
		}
	}

	@Override
	public boolean tooOldSeqId(String fromOffset) {
		if (fromOffset != null) {
			ReadOptions readOptions = new ReadOptions();
			readOptions.fillCache(false);
			synchronized (replaceDBDirLock) {
				DBIterator iterator = db.iterator(readOptions);
				iterator.seekToFirst();
				byte[] firstSeqId = null;
				if (iterator.hasNext() && (ServerConstants.initComparator()
						.compare((firstSeqId = iterator.next().getKey()), fromOffset.getBytes())) > 0) {
					log.warn("Too old seqId,unable to read from:" + fromOffset + ",server first seqId is:"
							+ new String(firstSeqId));
					return true;
				}
			}
		}
		return false;
	}

	private Date reOpenToClearDate = null;

	/**
	 * 清理磁盘
	 */
	@Override
	public void run() {
		ReadOptions readOpt = new ReadOptions();
		readOpt.fillCache(false);
		DBIterator iterator = this.db.iterator(readOpt);
		iterator.seekToFirst();
		long curTime = System.currentTimeMillis();
		log.info("{} begin to count", this.storageDir.getName());
		long curCount = StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), true).count();
		log.info("{} count result:{} use time:{}", this.storageDir.getName(), curCount,
				(System.currentTimeMillis() - curTime));
		if (curCount > ServerConstants.MAX_STORAGE_EVENT_COUNT) {
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
			log.info("pre count:{} in {} ,and cleared range data from:{} to:{},delete count:{}", curCount,
					this.storageDir.getName(), new String(firstKey), new String(lastKey),
					(curCount - ServerConstants.MAX_STORAGE_EVENT_COUNT));
			if (reOpenToClearDate == null) {
				reOpenToClearDate = DateUtils.randomFutureDate(50, 120);
				log.info("{} reOpenToClearDate set to:{}", this.storageDir.getName(),
						DateUtils.formatyyyyMMddHHmmss(reOpenToClearDate));
			} else if (new Date().compareTo(reOpenToClearDate) >= 0) {
				synchronized (replaceDBDirLock) {
					// 关闭现有的,并且重开
					try {
						log.info("close and reopen in order to release dir:{}", this.storageDir.getName());
						this.db.close();
						this.db = this.initDbInfo(false);
					} catch (IOException e) {
						log.info("error while close dir:{}", storageDir.getAbsolutePath());
						log.error("error while close dir:{}", e);
					}
				}
				reOpenToClearDate = DateUtils.randomFutureDate(50, 120);
				log.info("{} reOpenToClearDate set to:{}", this.storageDir.getName(),
						DateUtils.formatyyyyMMddHHmmss(reOpenToClearDate));
			}
		}
	}

}
