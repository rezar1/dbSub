package com.Rezar.dbSub.base.enums;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.github.shyiko.mysql.binlog.event.EventType;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月21日 下午2:55:18
 * @Desc 些年若许,不负芳华.
 *
 */
public enum ChangeType {

	INSERT(0, new EventType[] { EventType.EXT_WRITE_ROWS, EventType.WRITE_ROWS }),

	UPDATE(1, new EventType[] { EventType.EXT_UPDATE_ROWS, EventType.UPDATE_ROWS }),

	DELETE(2, new EventType[] { EventType.EXT_DELETE_ROWS, EventType.DELETE_ROWS }),

	QUERY(3),

	;

	public int type;
	private Set<EventType> subEventTypes;

	private ChangeType(int type) {
		this.type = type;
	}

	private ChangeType(int type, EventType[] subEventTypes) {
		this.type = type;
		this.subEventTypes = new HashSet<EventType>(Arrays.asList(subEventTypes));
	}

	private final static Map<EventType, ChangeType> eventTypeToChangeType = new HashMap<EventType, ChangeType>();

	private final static Map<Integer, ChangeType> changeTypeMap = new HashMap<>();

	static {
		for (ChangeType type : ChangeType.values()) {
			changeTypeMap.put(type.type, type);
			if (type.subEventTypes != null) {
				type.subEventTypes.stream().forEach(eventType -> {
					eventTypeToChangeType.put(eventType, type);
				});
			}
		}
	}

	public static ChangeType byEventType(EventType eventType) {
		return eventTypeToChangeType.get(eventType);
	}

	public static ChangeType valueOf(int changeType) {
		return changeTypeMap.get(changeType);
	}

}
