package com.Rezar.dbSub.server.infoPersistent;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
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
public class BinlogDataStorageWithLevelDB implements BinlogDataStorage {

	private File storageDir;
	private File curDbFile;
	private volatile DB db;
	private Object newMsgLock = new Object();
	private Object replaceDBDirLock = new Object();
	private int curStorageCount;
	private String curWriteSeqId;

	private static final String dirPathFormat = "/%s/%s/%s";

	private SyncEventProtoBufSerializer syncEventSerializer = new SyncEventProtoBufSerializer();

	@Override
	public void init(String dbIns, String db, String table) {
		this.storageDir = new File(
				ServerConstants.SAVE_BASE_FILE.getAbsolutePath() + String.format(dirPathFormat, dbIns, db, table));
		if (!storageDir.exists()) {
			storageDir.mkdirs();
		}
		this.db = initDbInfo(false);
		this.curStorageCount = readCurStorageCount();
		log.info("{}:{} - active db storage :{} event", db, table, this.curStorageCount);
	}

	private int readCurStorageCount() {
		ReadOptions readOpt = new ReadOptions();
		readOpt.fillCache(false);
		DBIterator iterator = this.db.iterator(readOpt);
		iterator.seekToFirst();
		int count = 0;
		if (iterator.hasNext()) {
			count++;
		}
		try {
			iterator.close();
		} catch (IOException e) {
			log.error("readCurStorageCount error:{}", e);
		}
		return count;
	}

	private DB initDbInfo(boolean newVersion) {
		try {
			Optional<Path> maxVersionDir = Files.list(Paths.get(storageDir.toURI()))
					.sorted(Comparator.comparing(Path::getFileName).reversed()).findFirst();
			curDbFile = maxVersionDir.orElseGet(() -> {
				return new File(storageDir, String.valueOf(System.currentTimeMillis())).toPath();
			}).toFile();
			if (newVersion) {
				curDbFile = new File(storageDir, String.valueOf(System.currentTimeMillis()));
			}
			Options options = new Options();
			options.comparator(ServerConstants.initComparator());
			options.createIfMissing(true);
			DB db = factory.open(curDbFile, options);
			DBIterator iterator = db.iterator();
			iterator.seekToLast();
			if (!newVersion) {
				curWriteSeqId = iterator.hasNext() ? (FromOffsetParser.parseNextSeqId(iterator.next().getKey())) : "0";
				IOUtils.closeQuietly(iterator);
				log.info("init from offset:{}", this.curWriteSeqId);
			}
			return db;
		} catch (DBException ex) {
			throw new ServerInitException("Resource temporarily unavailable:" + curDbFile.getAbsolutePath());
		} catch (IOException e) {
			log.error("BinlogDataStorageWithLevelDB init db error:{}", e);
			throw new ServerInitException("unknow exception while initDbInfo", e);
		}
	}

	/**
	 * 存储并分配存储自增id
	 */
	@Override
	public void storeAndFillStorageId(SyncEvent event) {
		if (event != null) {
			curStorageCount++;
			synchronized (replaceDBDirLock) {
				log.info("new event:{} isMarkEvent:{}", event.getSeqId(), event.getSeqId().endsWith("y"));
				// 进行实际数据存储,如果sourceData不等于null,必须要使用该sourceData
				this.db.put(event.getSeqId().getBytes(), event.getSourceData() != null ? event.getSourceData()
						: this.syncEventSerializer.serializeWrapperSeqId(event));
				this.curWriteSeqId = event.getSeqId();
			}
			synchronized (newMsgLock) {
				newMsgLock.notifyAll();
			}
			if (curStorageCount > ServerConstants.MAX_STORAGE_EVENT_COUNT) {
				newAllocateDirAndClear(event.getSeqId());
				curStorageCount = 0;
			}
		}
	}

	/**
	 * 当前缓存的数据超过限制,进行数据库目录变更并复制最近的数据到新目录(限制数额的一半),清理掉之前的目录
	 * 
	 * TODO
	 * 
	 * @param sequenceId
	 */
	private void newAllocateDirAndClear(String sequenceId) {
		// TODO 看下加锁范围是否可以优化
		synchronized (replaceDBDirLock) {
			DB oldDb = this.db;
			File oldDbFile = this.curDbFile;
			DB newDb = this.initDbInfo(true);
			ReadOptions readOpt = new ReadOptions();
			readOpt.fillCache(false);// 遍历中swap出来的数据，不应该保存在memtable中。
			DBIterator iterator = oldDb.iterator(readOpt);
			int holdCount = ServerConstants.MAX_STORAGE_EVENT_COUNT / 2;
			iterator.seekToLast();
			int readCount = 0;
			// 确保迭代最后面
			if (iterator.hasNext()) {
				iterator.next();
			}
			byte[] curKey = null;
			while (iterator.hasPrev() && (readCount) <= holdCount) {
				Entry<byte[], byte[]> next = iterator.prev();
				curKey = next.getKey();
				newDb.put(curKey, next.getValue());
				readCount++;
			}
			IOUtils.closeQuietly(iterator);
			log.info("newStart:{} copy count:{} newAllocateDir:{} replace old:{}", new String(curKey), readCount,
					this.curDbFile.getName(), oldDbFile.getName());
			this.db = newDb;
			try {
				TimeUnit.MILLISECONDS.sleep(3);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			Options options = new Options();
			try {
				oldDb.close();
				factory.destroy(oldDbFile, options);
			} catch (IOException e) {
				log.info("error while destroy old db:{}", oldDbFile.getAbsolutePath());
				log.error("error while destroy old db:{}", e);
			}
		}
	}

	@Override
	public MsgNextIterator iteratorFrom(String offset) {
		return new MsgIterator(offset);
	}

	public void close() throws IOException {
		this.db.close();
	}

	private class MsgIterator implements MsgNextIterator {

		volatile boolean stop;

		String tryReadOffset = "0";
		private SyncEventProtoBufSerializer syncEventSerializer = new SyncEventProtoBufSerializer();

		MsgIterator(String fromOffset) {
			DBIterator iterator = db.iterator();
			/**
			 * 1) 如果是从指定下标开始读 <br/>
			 * 读该下标的下一位数据
			 * 
			 * 2) 如果当前无数据,则最后从初始化index=0开始,后续直接从最新的writeSeqId开始读
			 */
			if (fromOffset != null) {
				iterator.seek(fromOffset.getBytes());
				this.tryReadOffset = FromOffsetParser.parseNextSeqId(fromOffset);
			} else {
				iterator.seekToLast();
				tryReadOffset = iterator.hasNext() ? (FromOffsetParser.parseNextSeqId(iterator.next().getKey()))
						: tryReadOffset;
			}
			log.info("MsgIterator for:{} and readIndex:{}", storageDir.getName(), tryReadOffset);
		}

		@Override
		public SyncEvent blockingTake() throws InterruptedException {
			try {
				while (!stop) {
					byte[] value = null;
					tryReadOffset = tryReadOffset == "0" ? curWriteSeqId : tryReadOffset;
					if ((value = (db.get(String.valueOf(tryReadOffset).getBytes()))) != null) {
						SyncEvent deserializeJustInitSeqId = syncEventSerializer.deserializeJustInitSeqId(value);
						this.tryReadOffset = FromOffsetParser.parseNextSeqId(deserializeJustInitSeqId.getSeqId());
						if (!deserializeJustInitSeqId.getSeqId().endsWith("y")) {
							// 是binlog非数据变更型消息(只用于offset衔接),所以只移动当前的this.tryReadOffset,不下发消息
							return deserializeJustInitSeqId; // 直接使用字节数组发送给下游,减少中间传输二次转序操作
						}
					}
					synchronized (newMsgLock) {
						if (this.tryReadOffset.compareTo(curWriteSeqId) < 0) {
							continue;
						}
						newMsgLock.wait();
					}
				}
			} catch (InterruptedException e) {
				log.info("InterruptedException");
				Thread.interrupted();// 向上传播中断信号
				throw e;
			}
			return null;
		}

		@Override
		public void close() {
			Thread.currentThread().interrupt();
			this.stop = true;
			log.info("MsgIterator exit");
		}
	}

}
