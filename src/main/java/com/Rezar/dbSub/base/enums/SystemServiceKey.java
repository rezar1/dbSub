package com.Rezar.dbSub.base.enums;

import com.Rezar.dbSub.base.event.BinlogServerEvent.SingleDbActorMessage;

import akka.actor.typed.receptionist.ServiceKey;

public class SystemServiceKey {

	public static final <T> ServiceKey<T> serviceKey(String dbIns, Class<T> clazz, String serviceName) {
		return ServiceKey.create(clazz, serviceName);
	}

	public static final ServiceKey<SingleDbActorMessage> dbInsServiceKey(String dbIns) {
		return serviceKey(dbIns, SingleDbActorMessage.class, "dbInsBinlogServer:" + dbIns);
	}

}
