package com.Rezar.dbSub.client.interfaces;

import com.Rezar.dbSub.base.enums.ChangeType;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月21日 下午2:51:47
 * @Desc 些年若许,不负芳华.
 *
 */
public interface ChangeDataHandler<T> {

	public boolean onEvent(T oldData, T newData, String eventMsgId, long timestamp, ChangeType changeType);

}
