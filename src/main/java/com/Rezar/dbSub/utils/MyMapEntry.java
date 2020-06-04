package com.Rezar.dbSub.utils;

import java.util.Map.Entry;

import lombok.Getter;

public class MyMapEntry<K, V> implements Entry<K, V> {

	@Getter
	private K key;
	@Getter
	private V value;

	public MyMapEntry(Entry<K, V> key) {
		this.key = key.getKey();
		this.value = key.getValue();
	}

	public MyMapEntry(K key, V value) {
		this.key = key;
		this.value = value;
	}

	@Override
	public V setValue(V value) {
		if (this.value == value) {
			return this.value;
		}
		V old = this.value;
		this.value = value;
		return old;
	}

}
