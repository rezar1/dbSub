package com.Rezar.dbSub.clientTest;

import java.util.concurrent.TimeUnit;

import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.Rezar.dbSub.client.init.ClientInit;
import com.Rezar.dbSub.client.init.ClientInitWithSpring;
import com.Rezar.dbSub.testHandler.IdeaExtAuditChangeDataHandler;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AkkaBinlogClientTest {

	public static void main(String[] args) throws InterruptedException {
		testWithoutSpring();
		// testWithSpring();
	}

	public static void testWithoutSpring() throws InterruptedException {
		ClientInit client = new ClientInit();
		IdeaExtAuditChangeDataHandler ideaExtAuditChangeDataHandler = new IdeaExtAuditChangeDataHandler();
		client.registerChangeDataHandler(ideaExtAuditChangeDataHandler);
		client.startListener(20, TimeUnit.SECONDS);
		log.info("client startup");
	}

	public static void testWithSpring() throws InterruptedException {
		ClassPathXmlApplicationContext application = new ClassPathXmlApplicationContext("eventHandlerBeans.xml");
		ClientInitWithSpring bean = application.getBean(ClientInitWithSpring.class);
		bean.startListener(10, TimeUnit.SECONDS);
		log.info("AkkaBinlogClientTest startup with resutlt:{}", bean.initSuccess());

		TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
		application.close();
	}

}
