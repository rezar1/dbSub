package com.Rezar.dbSub.client.interfaces;

import java.io.Closeable;

import com.Rezar.dbSub.base.dbInfo.TableMark;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2019年5月17日 下午4:33:54
 * @Desc 些年若许,不负芳华.
 *
 */
public interface OffsetRecorder extends Closeable {

	public String takeLastOffset(TableMark tableMark);

	public boolean saveLastOffset(TableMark tableMark, String seqId);

}
