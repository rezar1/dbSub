package com.Rezar.dbSub.client.config;

import org.apache.commons.lang3.builder.EqualsExclude;

import com.Rezar.dbSub.base.dbInfo.TableMark;
import com.Rezar.dbSub.base.enums.OffsetEnum;
import com.Rezar.dbSub.client.infoPersistent.DurableIdCreator;
import com.Rezar.dbSub.utils.GU;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class TableInitKey {

	@Getter
	private String dbIns;
	@Getter
	private String db;
	@Getter
	private String table;
	@Getter
	private String durableId;
	@Getter
	@EqualsExclude
	private String offset;

	public static TableInitKey from(SubTableInfo subTableInfo) {
		TableInitKey key = new TableInitKey();
		key.dbIns = subTableInfo.getDbInstanceName();
		key.db = subTableInfo.getDb();
		key.table = subTableInfo.getTableName();
		key.offset = subTableInfo.getOffset();
		key.durableId = DurableIdCreator.createDurableId(key.dbIns, key.db, key.table);
		return key;
	}

	public boolean isFromOffset() {
		return !(GU.isNullOrEmpty(offset) || OffsetEnum.CONTINUE.name().contentEquals(offset)
				|| OffsetEnum.LAST_POS.name().contentEquals(offset));
	}

	public TableMark toTableMark() {
		return new TableMark(dbIns, db, table);
	}

}
