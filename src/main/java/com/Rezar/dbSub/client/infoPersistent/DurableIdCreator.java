package com.Rezar.dbSub.client.infoPersistent;

/**
 * 获取客户端主机的唯一标识
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 23, 2020 4:10:24 PM
 * @Desc 些年若许,不负芳华.
 *
 */
public class DurableIdCreator {

	public static String createDurableId(String dbIns, String db, String table) {
		return ClientGroupInfo.CLIENT_GROUP_MARK + (String.format(":%s:%s:%s", dbIns, db, table));
	}

}
