package com.Rezar.dbSub.ser;

import java.io.IOException;
import java.util.LinkedHashMap;

import com.Rezar.dbSub.base.event.SyncEvent;
import com.Rezar.dbSub.utils.JacksonUtil;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;

public class AkkaProtobufSerializerTest {

	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
		SyncEvent event = new SyncEvent();
		event.setSeqId("1");
		event.setDb("Rezafdsfsdr");
		event.setEventType(1);
		event.setNewDatas(new LinkedHashMap<String, Object>());

		long start = System.currentTimeMillis();
		for (int i = 0; i < 15000; i++) {
//			byte[] serialize = ProtoBufSerializeUtils.serialize(event);
//			ProtoBufSerializeUtils.deserialize(serialize, SyncEvent.class);

			JacksonUtil.str2Obj(JacksonUtil.obj2Str(event), SyncEvent.class);

		}
		long end = System.currentTimeMillis();
		System.out.println("usTime:" + (end - start));

	}

}
