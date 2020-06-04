package com.Rezar.dbSub.server.infoPersistent;

import com.Rezar.dbSub.base.event.SyncEvent;
import com.Rezar.dbSub.utils.ProtoBufSerializeUtils;

/**
 * 序列化 SyncEvent
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 27, 2020 11:25:24 PM
 * @Desc 些年若许,不负芳华.
 *
 */
public class SyncEventValueSeriaUtil {

	public static byte[] serialEventData(SyncEvent event) {
		ProtoBufSerializeUtils.serialize(event);
		return null;
	}

}
