package com.Rezar.dbSub.client.config;

import java.util.ArrayList;
import java.util.List;

import com.Rezar.dbSub.utils.SubInfoMark;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月24日 上午11:27:47
 * @Desc 些年若许,不负芳华.
 *
 */
@Data
public class SubTableInfo {

	private String dbInstanceName;
	private String db;
	private String tableName;
	private String offset = "LAST_POS";
	@SuppressWarnings("rawtypes")
	@JsonIgnore
	private Class realEntityClass;
	@JsonIgnore
	private String entityClass;
	@JsonIgnore
	private List<SingleSubHandler> handlers = new ArrayList<>();

	public String markName() {
		return SubInfoMark.getSubInfoMark(dbInstanceName, db, tableName);
	}

	public void merge(SubTableInfo subTableInfo) {
		String otherFromOffset = subTableInfo.getOffset();
		if (otherFromOffset != null) {
			this.offset = otherFromOffset;
		}
		List<SingleSubHandler> preHandleres = subTableInfo.getHandlers();
		for (SingleSubHandler preHandler : preHandleres) {
			if (!handlers.contains(preHandler)) {
				handlers.add(preHandler);
			}
		}
	}

}
