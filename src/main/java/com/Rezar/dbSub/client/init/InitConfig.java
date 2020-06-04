package com.Rezar.dbSub.client.init;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.Rezar.dbSub.base.enums.OffsetEnum;
import com.Rezar.dbSub.client.config.SubTableInfo;
import com.Rezar.dbSub.utils.GU;
import com.Rezar.dbSub.utils.SubInfoMark;

public class InitConfig {

	private Map<String, String> offsetModeMap = new HashMap<>();
	private String forceToAll;

	private InitConfig() {
	}

	/**
	 * 
	 * @param forceToAll        如果指定,则启动时全表都使用该模式连接binlog服务端
	 * @param offsetModelConfig 格式:table1:OffsetMode1|table2:OffsetMode2_.....
	 *                          (表:模式) 以(|)符号连接起来,用于指定特定表的offset模式,单表的offset模式默认为
	 *                          LAST_POS
	 */
	public InitConfig(String serverName, String forceToAll, String offsetModelConfig) {
		this.withServerName(serverName);
		if (GU.notNullAndEmpty(forceToAll) && OffsetEnum.valueOf(forceToAll) != null) {
			this.forceToAll = forceToAll;
		} else if (GU.notNullAndEmpty(offsetModelConfig)) {
			Arrays.stream(offsetModelConfig.trim().split("\\|")).map(singleTable -> singleTable.split(":"))
					.filter(config -> OffsetEnum.valueOf(config[1].toUpperCase()) != null).forEach(config -> {
						this.offsetModeMap.put(config[0], config[1].toUpperCase());
					});
		}
	}

	public Optional<String> hasForceToAllMode() {
		return this.forceToAll == null ? Optional.empty() : Optional.of(this.forceToAll);
	}

	public Optional<String> hasSpecialMode(String dbIns, String db, String table) {
		String key = SubInfoMark.getSubInfoMark(dbIns, db, table);
		return this.offsetModeMap.containsKey(key) ? Optional.of(this.offsetModeMap.get(key)) : Optional.empty();
	}

	public InitConfig offsetModeForceToAll(OffsetEnum offsetMode) {
		forceToAll = offsetMode == null ? null : offsetMode.name();
		return this;
	}

	public InitConfig specialTableOffsetMode(String dbIns, String db, String table, OffsetEnum offsetEnum) {
		this.offsetModeMap.put(SubInfoMark.getSubInfoMark(dbIns, db, table), offsetEnum.name());
		return this;
	}

	public static InitConfig init() {
		return new InitConfig();
	}

	public void config(SubTableInfo config) {
		if (this.forceToAll != null) {
			config.setOffset(forceToAll);
		} else if (this.offsetModeMap.containsKey(
				SubInfoMark.getSubInfoMark(config.getDbInstanceName(), config.getDb(), config.getTableName()))) {
			config.setOffset(this.offsetModeMap.get(
					SubInfoMark.getSubInfoMark(config.getDbInstanceName(), config.getDb(), config.getTableName())));
		}
	}

	public InitConfig withServerName(String serverName) {
		System.setProperty("ClientId", GU.notNullAndEmpty(serverName) ? serverName : "default");
		return this;
	}

}
