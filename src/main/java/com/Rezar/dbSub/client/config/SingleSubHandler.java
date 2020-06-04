package com.Rezar.dbSub.client.config;

import com.Rezar.dbSub.base.enums.ChangeType;
import com.Rezar.dbSub.client.interfaces.ChangeDataHandler;

import lombok.Data;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月21日 下午2:46:51
 * @Desc 些年若许,不负芳华.
 *
 */
@Data
public class SingleSubHandler {

	private String filter;
	private ChangeType[] acceptChangeTypes = new ChangeType[] { ChangeType.INSERT, ChangeType.UPDATE,
			ChangeType.DELETE };
	@SuppressWarnings("rawtypes")
	private ChangeDataHandler changeDataHandler;

	public String getFilter() {
		return this.filter;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SingleSubHandler other = (SingleSubHandler) obj;
		if (changeDataHandler == null) {
			if (other.changeDataHandler != null)
				return false;
		} else if (!changeDataHandler.equals(other.changeDataHandler))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((changeDataHandler == null) ? 0 : changeDataHandler.hashCode());
		return result;
	}

}
