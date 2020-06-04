package com.Rezar.dbSub.client.init;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.springframework.core.annotation.AnnotationUtils;

import com.Rezar.dbSub.base.EventHandlerAnnot;
import com.Rezar.dbSub.base.enums.ChangeType;
import com.Rezar.dbSub.base.exceptions.ServerInitException;
import com.Rezar.dbSub.client.akkaSystem.MainClientBinlogService;
import com.Rezar.dbSub.client.config.SingleDbInsClientConfigInfo;
import com.Rezar.dbSub.client.config.SingleSubHandler;
import com.Rezar.dbSub.client.config.SubTableInfo;
import com.Rezar.dbSub.client.config.TableBinlogProcessor;
import com.Rezar.dbSub.client.config.TableInitKey;
import com.Rezar.dbSub.client.interfaces.BinlogServerStatusListener.OnDbDown;
import com.Rezar.dbSub.client.interfaces.BinlogServerStatusListener.OnDbUp;
import com.Rezar.dbSub.client.interfaces.ChangeDataHandler;
import com.Rezar.dbSub.utils.GU;
import com.Rezar.dbSub.utils.SubInfoMark;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 27, 2020 4:19:36 PM
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public class ClientInit {

	private MainClientBinlogService mainClientBinlogServer;

	private List<OnDbUp> dbUpListener = new ArrayList<>();
	private List<OnDbDown> dbDownListener = new ArrayList<>();

	private List<SubTableInfo> parseConfig = new ArrayList<>();
	private String akkaClinetConfig;

	protected InitConfig initConfig;

	public ClientInit() {
		this(null, "default");
	}

	public ClientInit(String akkaClinetConfig, String serverName) {
		this(akkaClinetConfig, InitConfig.init().withServerName(serverName));
	}

	public ClientInit(String akkaClinetConfig, InitConfig initConfig) {
		this.akkaClinetConfig = akkaClinetConfig;
		this.initConfig = initConfig;
	}

	private volatile boolean initSucc = false;
	private volatile Semaphore initLock = new Semaphore(1);

	public void startListener() throws ServerInitException {
		this.startListener(-1l, null);
	}

	/**
	 * @throws InterruptedException
	 */
	public void startListener(long timeout, TimeUnit timeunit) throws ServerInitException {
		if (initSucc || !initLock.tryAcquire()) {
			log.info("startListener before , do not start again");
			return;
		}
		if (GU.isNullOrEmpty(this.parseConfig)) {
			log.info("empty subTableInfo config");
			return;
		}
		List<SingleDbInsClientConfigInfo> allSingleDbClientConfigInfo = this.configSubTableInfo().stream()
				.collect(Collectors.groupingBy(SubTableInfo::getDbInstanceName)).entrySet().stream()
				.map(entry -> new SingleDbInsClientConfigInfo(entry.getKey(),
						entry.getValue().stream()
								.collect(Collectors.toMap(TableInitKey::from, TableBinlogProcessor::fromSubTableInfo))))
				.collect(Collectors.toList());
		boolean needWaith = timeout != -1;
		Semaphore semaphore = new Semaphore(allSingleDbClientConfigInfo.size());
		if (needWaith) {
			semaphore.drainPermits();
			OnDbUp allDbUpListener = dbIns -> {
				semaphore.release();
			};
			this.dbUpListener.add(allDbUpListener);
		}
		this.mainClientBinlogServer = new MainClientBinlogService(this.akkaClinetConfig, allSingleDbClientConfigInfo,
				this.dbUpListener, this.dbDownListener);
		try {
			this.mainClientBinlogServer.startup();
			if (needWaith && !semaphore.tryAcquire(allSingleDbClientConfigInfo.size(), timeout, timeunit)) {
				throw new ServerInitException("can not connect to all dbIns binlog server");
			}
			initSucc = true;
		} catch (ServerInitException ex) {
			throw ex;
		} catch (Exception e) {
			throw new ServerInitException("unknow exception", e);
		}
		log.info("clientInit over:{}", initSucc);
	}

	private List<SubTableInfo> configSubTableInfo() {
		this.parseConfig.forEach(this.initConfig::config);
		return tryConfigSubTableInfo(this.parseConfig);
	}

	protected List<SubTableInfo> tryConfigSubTableInfo(List<SubTableInfo> subTableInfos) {
		return subTableInfos;
	}

	@SuppressWarnings("rawtypes")
	public final void registerChangeDataHandler(ChangeDataHandler<?> changeDataHandler) {
		if (changeDataHandler == null) {
			return;
		}
		Class<? extends Object> beanClass = changeDataHandler.getClass();
		EventHandlerAnnot annot;
		if ((annot = AnnotationUtils.findAnnotation(beanClass, EventHandlerAnnot.class)) != null) {
			String dbInstance = annot.dbInstance();
			String db = annot.db();
			String tableName = annot.tableName();
			String filter = annot.filter();
			ChangeType[] acceptType = annot.acceptType();
			if (GU.isNullOrEmpty(acceptType)) {
				acceptType = new ChangeType[] { ChangeType.INSERT, ChangeType.UPDATE, ChangeType.DELETE };
			}
			Type[] genericInterfaces = changeDataHandler.getClass().getGenericInterfaces();
			Type changeDataHandlerType = null;
			if (genericInterfaces.length != 0) {
				for (Type type : genericInterfaces) {
					if (type.getTypeName().startsWith(ChangeDataHandler.class.getName())) {
						changeDataHandlerType = type;
						break;
					}
				}
			} else {
				changeDataHandlerType = genericInterfaces[0];
			}
			Class entityClass = (Class) ((ParameterizedType) changeDataHandlerType).getActualTypeArguments()[0];
			String markName = SubInfoMark.getSubInfoMark(dbInstance, db, tableName);
			SubTableInfo subTableInfo = this.findSubTableInfo(markName);
			if (subTableInfo.getDbInstanceName() == null) { // 未初始化
				subTableInfo = new SubTableInfo();
				subTableInfo.setDbInstanceName(dbInstance);
				subTableInfo.setDb(db);
				subTableInfo.setTableName(tableName);
				subTableInfo.setRealEntityClass(entityClass);
				this.parseConfig.add(subTableInfo);
			}
			if (entityClass != subTableInfo.getRealEntityClass()) {
				log.error(
						"current ChangeDataHandler:{} with entity class:{} not match pre entityClass:{}, please check it",
						beanClass.getName(), entityClass, subTableInfo.getRealEntityClass());
			} else {
				SingleSubHandler singleHandler = new SingleSubHandler();
				singleHandler.setFilter(filter);
				singleHandler.setAcceptChangeTypes(acceptType);
				singleHandler.setChangeDataHandler((ChangeDataHandler) changeDataHandler);
				subTableInfo.getHandlers().add(singleHandler);
			}
		}
	}

	public final void registerSubTableInfo(SubTableInfo subTableInfo) {
		if (subTableInfo == null) {
			return;
		}
		SubTableInfo findSubTableInfo = this.findSubTableInfo(subTableInfo.markName());
		if (findSubTableInfo != null) {
			findSubTableInfo.merge(subTableInfo);
		} else {
			this.parseConfig.add(subTableInfo);
		}
	}

	public final void registerOnDbUpListener(OnDbUp onDbUp) {
		if (onDbUp == null) {
			return;
		}
		this.dbUpListener.add(onDbUp);
	}

	public final void registerOnDbDownListener(OnDbDown onDbDown) {
		if (onDbDown == null) {
			return;
		}
		this.dbDownListener.add(onDbDown);
	}

	private SubTableInfo findSubTableInfo(String markName) {
		return this.parseConfig.parallelStream().filter(info -> info.markName().contentEquals(markName)).findAny()
				.orElseGet(SubTableInfo::new);
	}

	public boolean initSuccess() {
		return this.initSucc;
	}

	public void destory() {
		// 停止binlog服务
		this.mainClientBinlogServer.stop();
	}
}
