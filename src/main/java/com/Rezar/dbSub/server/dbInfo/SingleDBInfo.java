package com.Rezar.dbSub.server.dbInfo;

import java.util.HashMap;
import java.util.Map;

import com.Rezar.dbSub.base.dbInfo.TableInfo;

import lombok.Data;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月26日 下午3:37:05
 * @Desc 些年若许,不负芳华.
 *
 */
@Data
public class SingleDBInfo {

	private String dbName;
	private Map<String, TableInfo> tableInfoMapper = new HashMap<>();

}
