package com.Rezar.dbSub.client.init;

import javax.annotation.PreDestroy;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;

import com.Rezar.dbSub.base.enums.OffsetEnum;
import com.Rezar.dbSub.client.config.SubTableInfo;
import com.Rezar.dbSub.client.interfaces.BinlogServerStatusListener.OnDbDown;
import com.Rezar.dbSub.client.interfaces.BinlogServerStatusListener.OnDbUp;
import com.Rezar.dbSub.client.interfaces.ChangeDataHandler;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time May 27, 2020 4:43:34 PM
 * @Desc 些年若许,不负芳华.
 *
 *       客户端启动类,配置信息从Spring容器中获取
 *
 */
public class ClientInitWithSpring extends ClientInit implements BeanPostProcessor {

	public ClientInitWithSpring() {
		super();
	}

	/**
	 * @param akkaConfig akkaCluster配置
	 * @param serverName 标识客户端服务名称
	 * @param forceToAll offsetMode
	 */
	public ClientInitWithSpring(String akkaConfig, String serverName, String forceToAll) {
		this(akkaConfig,
				InitConfig.init().withServerName(serverName).offsetModeForceToAll(OffsetEnum.valueOf(forceToAll)));
	}

	public ClientInitWithSpring(String akkaClinetConfig, InitConfig initConfig) {
		super(akkaClinetConfig, initConfig);
	}

	@PreDestroy
	public void destory() {
		super.destory();
	}

	@Override
	public Object postProcessBeforeInitialization(Object bean, String beanName) throws BeansException {
		return bean;
	}

	@SuppressWarnings({ "rawtypes" })
	@Override
	public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
		if (bean instanceof ChangeDataHandler) {
			super.registerChangeDataHandler((ChangeDataHandler) bean);
		} else if (bean instanceof SubTableInfo) {
			super.registerSubTableInfo((SubTableInfo) bean);
		} else if (bean instanceof OnDbDown) {
			super.registerOnDbDownListener((OnDbDown) bean);
		} else if (bean instanceof OnDbUp) {
			super.registerOnDbUpListener((OnDbUp) bean);
		}
		return bean;
	}

}
