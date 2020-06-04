package com.Rezar.dbSub.utils;

import java.lang.reflect.Array;

import com.Rezar.dbSub.base.event.SyncEvent;
import com.lmax.disruptor.EventHandler;

public class ArrayUtils {

	@SuppressWarnings("unchecked")
	public static <T> T[] newInstance(Class<T> clazz, int size) {
		return (T[]) Array.newInstance(clazz, size);
	}
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		EventHandler<SyncEvent>[] array = newInstance(EventHandler.class, 1);
		System.out.println(array.length);
		
	}

}
