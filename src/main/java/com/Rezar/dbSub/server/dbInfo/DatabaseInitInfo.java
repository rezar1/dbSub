package com.Rezar.dbSub.server.dbInfo;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.ToStringExclude;

import com.Rezar.dbSub.base.dbInfo.TableInfo;
import com.Rezar.dbSub.base.enums.ChangeType;
import com.Rezar.dbSub.utils.GU;
import com.Rezar.dbSub.utils.PatternUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;
import lombok.Getter;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年12月6日 下午2:38:20
 * @Desc 些年若许,不负芳华.
 *
 */
@Data
public class DatabaseInitInfo {

	private String dbInstanceName;
	private String url;
	private String username;
	@ToStringExclude
	@JsonIgnore
	private String password;
	private String host;
	private Integer port;
	private String clientId;
	private String gtidMode;
	private String acceptEvent;
	private String fromBinlogFile;
	private String fromBinlogOffset;
	private List<DBInitInfo> subDBInitInfos = new ArrayList<>();
	private Map<String, DBInitInfo> subDBInitInfoMapper = new HashMap<>();

	@JsonIgnore
	@Getter
	private HashSet<ChangeType> awareChangeType = new HashSet<>();

	public void parseConfig() {
		if (GU.notNullAndEmpty(this.acceptEvent)) {
			this.awareChangeType.addAll(Arrays.stream(this.acceptEvent.split(",")).map(Integer::parseInt)
					.map(ChangeType::valueOf).collect(Collectors.toList()));
		}
		for (DBInitInfo dbInitInfo : this.subDBInitInfos) {
			this.subDBInitInfoMapper.put(dbInitInfo.getDb(), dbInitInfo);
		}
		if (GU.notNullAndEmpty(this.fromBinlogFile) && !PatternUtils.isBinlogFile(this.fromBinlogFile)) {
			throw new IllegalArgumentException("无法识别的binlog文件名:[" + this.fromBinlogFile + "]");
		}
		if (GU.notNullAndEmpty(this.fromBinlogOffset) && !PatternUtils.isBinlogOffset(this.fromBinlogOffset)) {
			throw new IllegalArgumentException("无法识别的binlog下标:[" + this.fromBinlogOffset + "]");
		}
		this.parseUrl();
	}

	public void parseUrl() {
		String tmpUrl = this.url.replace("jdbc:", "");
		URI uri = URI.create(tmpUrl);
		this.host = uri.getHost();
		this.port = uri.getPort();
	}

	/**
	 * @return
	 */
	public boolean isGtidMode() {
		return Boolean.parseBoolean(this.gtidMode);
	}

	/**
	 * @param db
	 * @return
	 */
	public DBInitInfo findDBInfo(String db) {
		return this.subDBInitInfoMapper.get(db);
	}

	/**
	 * @param database
	 * @param tableName
	 * @return
	 */
	public TableInfo findTableInfo(String database, String tableName) {
		DBInitInfo findDBInfo = this.findDBInfo(database);
		if (findDBInfo != null) {
			return findDBInfo.findTableInfo(tableName);
		}
		return null;
	}

	public boolean containSubTableInfo(String db, String table) {
		return this.subDBInitInfoMapper.containsKey(db) ? this.subDBInitInfoMapper.get(db).containeSubTable(table)
				: false;
	}

	public boolean awareChangeType(ChangeType changeType) {
		return this.awareChangeType.isEmpty() ? true : this.awareChangeType.contains(changeType);
	}

}
