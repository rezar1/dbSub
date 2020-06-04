package com.Rezar.dbSub.serverTest;

import java.util.Scanner;

import com.Rezar.dbSub.base.ServerConstants;
import com.Rezar.dbSub.server.akkaSystem.MainBinlogServer;

public class AkkaServerTest {

	public static void main(String[] args) throws Exception {
		ServerConstants.MAX_STORAGE_EVENT_COUNT = 200000;
		MainBinlogServer server = new MainBinlogServer();
		server.init("binlogAkkaServer", "initBinlog.xml", false);
		server.startup();
		Scanner sc = new Scanner(System.in);
		sc.nextLine();
		sc.close();
		server.destory();
	}

}
