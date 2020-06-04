package com.Rezar.dbSub.ser;

import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedHashMap;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import com.Rezar.dbSub.base.event.SyncEvent;
import com.Rezar.dbSub.utils.ProtoBufSerializeUtils;
import com.Rezar.dbSub.utils.SyncEventProtoBufSerializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProtoBufSerializerTest {

	public static void main(String[] args) {
		testTimeStampIfRight();
//		testTImes();
	}

	private static void testTimeStampIfRight() {
		SyncEventProtoBufSerializer ser = new SyncEventProtoBufSerializer();
		TestEventMsg result = null;
		TestEventMsg message = TestEventMsg.randomMsg();
		log.info("message:{}", message);
		byte[] serByte = ser.serialize(message);
		System.out.println("字节长度:" + serByte.length);
		result = ProtoBufSerializeUtils.deserialize(serByte, TestEventMsg.class);
		log.info("result:{}", result);
	}

	public static void testTImes() {
		TestEventMsg result = null;
		TestEventMsg message = TestEventMsg.randomMsg();
		log.info("message:{}", message);
		byte[] serByte = ProtoBufSerializeUtils.serialize(message);
		System.out.println("字节长度:" + serByte.length);
		result = ProtoBufSerializeUtils.deserialize(serByte, TestEventMsg.class);

		log.info("result:{}", result);

		TimeUtil watch = new TimeUtil();
		TimeUtil watch2 = new TimeUtil();

		// 这里设置测试次数
		for (int i = 0; i < 1000000; i++) {
			// timeUtil.init();
			watch.start();
			serByte = ProtoBufSerializeUtils.serialize(message);
			watch.end();
			// System.out.println("序列化时间："+ timeUtil.getAvrTimeUs() + " Us");

			watch2.start();
			result = ProtoBufSerializeUtils.deserialize(serByte, TestEventMsg.class);
			watch2.end();
		}
		System.out.println("序列化时间：" + watch.getAvrTimeUs() + " Us");
		System.out.println("反序列化时间：" + watch2.getAvrTimeUs() + " Us");

		System.out.println("结果:" + result);
	}

	static class TestEventMsg extends SyncEvent {

		public static TestEventMsg randomMsg() {
			TestEventMsg msg = new TestEventMsg();
			msg.setDb(RandomStringUtils.random(10, "abcdedfhijklmnopqrstuvwxyz"));
			msg.setTimestamp(System.currentTimeMillis());
			msg.oldDatas = new LinkedHashMap<String, Object>();
			msg.oldDatas.put("info1",
					RandomStringUtils.random(RandomUtils.nextInt(10, 20), "abcdedfhijklmnopqrstuvwxyz"));
			msg.oldDatas.put("info2", RandomUtils.nextInt(20, 89));
			msg.oldDatas.put("createTime", new Timestamp(new Date().getTime()));
			return msg;
		}

	}

	static class TimeUtil {

		private long startTime;
		private long endTime;
		private long timeSum;
		private long count;

		public void init() {
			timeSum = 0;
			count = 0;
		}

		public void start() {
			startTime = System.nanoTime();

		}

		public void end() {
			endTime = System.nanoTime();
			timeSum += (endTime - startTime);
			count++;
		}

		public long getAvrTimeNs() {
			return (timeSum / count);
		}

		public long getAvrTimeUs() {
			return (timeSum / count) / 1000;
		}

		public long getAvrTimeMs() {
			return (timeSum / count) / 1000000;
		}

	}

}
