package com.Rezar.dbSub.base;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

import org.iq80.leveldb.DBComparator;

import com.Rezar.dbSub.utils.GU;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月28日 上午11:13:15
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public class ServerConstants {

	public static File SAVE_BASE_FILE;

	public static String serverUUID = null;

	public static final int OLD = 0;
	public static final int NEW = 1;

	public static final byte INSERT = 0;
	public static final byte UPDATE = 1;
	public static final byte DELETE = 2;

	public static int MAX_STORAGE_EVENT_COUNT = 50000 * 2; // 10w的数据,每次清理后只保留该数的1/2

	static {
		initBaseDir("");
	}

	public static void main(String[] args) {
		System.out.println(ServerConstants.SAVE_BASE_FILE.getAbsolutePath());
	}

	public static void initBaseDir(String dir) {
		String catalinaHome = System.getProperty("catalina.home");
		if (GU.notNullAndEmpty(catalinaHome)) {
			SAVE_BASE_FILE = new File(catalinaHome);
		} else {
			SAVE_BASE_FILE = new File(dir);
		}
		log.info("change ServerConstants BaseDir to:{}", ServerConstants.SAVE_BASE_FILE.getAbsolutePath());
	}

	public static File getUUIDFile() {
		File uuidFile = new File(SAVE_BASE_FILE, "_bin_server.uuid");
		return uuidFile;
	}

	public static void initServerUUIDFile() {
		File uuidFile = getUUIDFile();
		uuidFile.deleteOnExit();
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(new FileWriter(uuidFile, false), true);
			pw.write(serverUUID = UUID.randomUUID().toString());
		} catch (IOException e) {
			log.error(
					String.format("failure to initServerUUIDFile with path:%s failure:{} ", uuidFile.getAbsolutePath()),
					e);
		} finally {
			if (pw != null) {
				pw.close();
			}
		}
	}

	/**
	 * @return
	 */
	public static String readUUIDFromFile() {
		File uuidFile = new File(SAVE_BASE_FILE, "_bin_server.uuid");
		if (!uuidFile.exists()) {
			throw new IllegalStateException(uuidFile.getAbsolutePath() + " not exists");
		}
		String uuidRet = "";
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(uuidFile));
			uuidRet = reader.readLine();
		} catch (IOException e) {
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		log.info("uuidRet is:{}", uuidRet);
		return uuidRet.trim();
	}
	
	public final static DBComparator COMPARATOR = initComparator();

	public static DBComparator initComparator() {
		return initComparator(false);
	}

	public static DBComparator initComparator(boolean reverse) {
		return new DBComparator() {
			@Override
			public int compare(byte[] o1, byte[] o2) {
				String key1 = new String(o1);
				String key2 = new String(o2);
				String[] info1 = key1.split("_");
				String[] info2 = key2.split("_");
				long res = 0;
				for (int index = 0; index < info1.length; index++) {
					res = Long.parseLong(info1[index]) - (Long.parseLong(info2[index]));
					if (res != 0) {
						break;
					}
				}
				res = res == 0 ? 0 : (res > 0 ? 1 : -1);
				return (int) (reverse ? -res : res);
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
	}

}
