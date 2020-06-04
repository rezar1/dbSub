package com.Rezar.dbSub.client.infoPersistent;

import java.io.IOException;
import java.util.function.Supplier;

import com.Rezar.dbSub.client.interfaces.OffsetRecorder;

/**
 * 
 * 存储客户端运行时的一些数据
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 23, 2020 6:06:30 PM
 * @Desc 些年若许,不负芳华.
 *
 */
public class ClientDataStoreInfo {

	public static final ClientDataStoreInfo STORE = new ClientDataStoreInfo();

	private ClientDataStoreInfo() {
	}

	private volatile OffsetRecorder offsetRecorder;
	private static Supplier<OffsetRecorder> storeSupplier = null;

	static {
		storeSupplier = withConfig();
		if (storeSupplier == null) {
			storeSupplier = () -> new OffsetRecorderByLevelDB();
		}
	}

	public OffsetRecorder initDbOffsetRecorder() {
		if (this.offsetRecorder == null) {
			synchronized (this) {
				if (this.offsetRecorder == null) {
					this.offsetRecorder = storeSupplier.get();
				}
			}
		}
		return this.offsetRecorder;
	}

	public void onClose() {
		try {
			this.offsetRecorder.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 看能不能配置
	 * 
	 * @return
	 */
	private static Supplier<OffsetRecorder> withConfig() {
		return null;
	}

}
