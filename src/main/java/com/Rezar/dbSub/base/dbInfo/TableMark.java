package com.Rezar.dbSub.base.dbInfo;

import org.apache.commons.lang3.builder.EqualsExclude;

import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
public class TableMark {

	@Getter
	private String dbIns;
	@Getter
	private String db;
	@Getter
	private String table;
	@Getter
	private String strMarkId;
	@Getter
	@EqualsExclude
	private byte[] binaryMarkId;

	public TableMark(String dbIns, String db, String table) {
		super();
		this.dbIns = dbIns;
		this.db = db;
		this.table = table;
		this.strMarkId = this.dbIns + ":" + this.db + ":" + this.table;
		this.binaryMarkId = this.strMarkId.getBytes();
	}

	@Override
	public String toString() {
		return strMarkId;
	}
}
