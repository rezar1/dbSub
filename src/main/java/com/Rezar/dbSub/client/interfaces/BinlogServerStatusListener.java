package com.Rezar.dbSub.client.interfaces;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年12月9日 下午12:23:04
 * @Desc 些年若许,不负芳华.
 *
 */
public interface BinlogServerStatusListener {

	@FunctionalInterface
	public static interface OnDbUp {
		public void onDbUp(String dbIns);
	}

	@FunctionalInterface
	public static interface OnDbDown {
		public void onDbDown(String dbIns);
	}

}
