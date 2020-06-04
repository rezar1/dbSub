package com.Rezar.dbSub;

import java.io.File;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.ComponentScan;

import com.Rezar.dbSub.base.ServerConstants;
import com.Rezar.dbSub.commandline.MysqlBinlogServerOptions;
import com.Rezar.dbSub.server.akkaSystem.MainBinlogServer;
import com.Rezar.dbSub.utils.GU;
import com.beust.jcommander.JCommander;

@SpringBootApplication
@ComponentScan(basePackages = { "com.Rezar.dbSub.server.akkaSystem" })
public class DbSubApplication implements org.springframework.boot.CommandLineRunner {

	@Autowired
	private MainBinlogServer mainBinlogServer;

	public static void main(String[] args) {
		new SpringApplicationBuilder(DbSubApplication.class).web(WebApplicationType.NONE).run(args);
	}

	public void run(String... args) throws Exception {
		MysqlBinlogServerOptions options = new MysqlBinlogServerOptions();
		JCommander jCommander = JCommander.newBuilder().programName("akkaBinlogServer").addObject(options).build();
		jCommander.parse(args);
		if (options.isHelp()) {
			jCommander.usage();
			return;
		}
		// 设置文件存储目录
		String workFloder = options.getWorkFloder();
		if (GU.isNullOrEmpty(workFloder)) {
			workFloder = new File(".").getAbsolutePath();
		}
		ServerConstants.initBaseDir(workFloder);
		String maxStoreEventCount = options.getMaxStoreEventCount();
		int maxStoreCount = 100000;
		if (GU.notNullAndEmpty(maxStoreEventCount) && StringUtils.isNumeric(maxStoreEventCount)) {
			maxStoreCount = Integer.parseInt(maxStoreEventCount);
			maxStoreCount = (maxStoreCount <= 10000 || maxStoreCount >= 1000000) ? 100000 : maxStoreCount;
		}
		ServerConstants.MAX_STORAGE_EVENT_COUNT = maxStoreCount;

		mainBinlogServer.init(options.getClusterConfigFile(), options.getBinglogConfigFile(), true);
		mainBinlogServer.startup();
	}

}
