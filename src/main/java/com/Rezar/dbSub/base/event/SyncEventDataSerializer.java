package com.Rezar.dbSub.base.event;

import com.Rezar.dbSub.utils.ProtoBufSerializeUtils;
import com.Rezar.dbSub.utils.SyncEventProtoBufSerializer;

import akka.serialization.JSerializer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SyncEventDataSerializer extends JSerializer {

	private static ThreadLocal<SyncEventProtoBufSerializer> serializer = new ThreadLocal<SyncEventProtoBufSerializer>() {
		protected SyncEventProtoBufSerializer initialValue() {
			return SyncEventProtoBufSerializer.instance();
		}
	};

	@Override
	public int identifier() {
		return 111222;
	}

	@Override
	public boolean includeManifest() {
		return false;
	}

	@Override
	public byte[] toBinary(Object o) {
		if (o instanceof SyncEvent) {
			if ((((SyncEvent) o).getSourceData()) != null) {
				return ((SyncEvent) o).getSourceData();
			} else {
				return serializer.get().serialize((SyncEvent) o);
			}
		}
		return ProtoBufSerializeUtils.serialize(o);
	}

	@Override
	public Object fromBinaryJava(byte[] bytes, Class<?> manifest) {
		try {
			return serializer.get().deserializeFull(bytes);
		} catch (Exception e) {
			log.error("error while fromBinaryJava:{}", e);
		}
		return null;
	}

}
