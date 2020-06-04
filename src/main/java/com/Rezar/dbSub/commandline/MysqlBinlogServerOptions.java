package com.Rezar.dbSub.commandline;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.annotation.JsonIgnore;

import lombok.Data;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年10月13日 下午4:10:26
 * @Desc 些年若许,不负芳华.
 *
 */
@Data
@Parameters(commandNames = { "mysqlbinlog" }, separators = "=", commandDescription = "mysql databus")
public class MysqlBinlogServerOptions {

	@JsonIgnore
	@Parameter(names = { "--help", "-h" }, help = true)
	private boolean help;
	@Parameter(names = { "-binlogConfig", "-bc" }, description = "启动的binlog配置文件(.xml)", required = true)
	private String binglogConfigFile;
	@Parameter(names = { "-clusterConfig", "-cc" }, description = "启动的akka集群的配置文件(.conf)", required = true)
	private String clusterConfigFile;
	@Parameter(names = { "-workFolder", "-wf" }, description = "服务存储文件的目录(默认为当前目录--[.]代表当前目录)", required = false)
	private String workFloder = ".";
	@Parameter(names = { "-maxStore", "-ms" }, description = "存储的单表的记录数上限,超过该数会清理超出的历史数据", required = false)
	private String maxStoreEventCount = "100000";

}
