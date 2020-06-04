package com.Rezar.dbSub.client.config;

import java.util.Map;

import com.Rezar.dbSub.utils.GU;
import com.Rezar.dbSub.utils.beanUtil.BeanInvokeUtils;
import com.Rezar.dbSub.utils.beanUtil.BeanMethodInvoke;
import com.Rezar.dbSub.utils.beanUtil.Invoker;

import lombok.Data;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月21日 下午3:27:50
 * @Desc 些年若许,不负芳华.
 *
 */
@Data
public class EntityParser {

	private String className;
	private Class<?> entryClass;
	@SuppressWarnings("rawtypes")
	private BeanMethodInvoke beanInvoker;

	@SuppressWarnings("rawtypes")
	public EntityParser(Class realClass) throws ClassNotFoundException {
		this.className = realClass.getName();
		this.entryClass = realClass;
		this.initEntryInfo();
	}

	public EntityParser(String className) throws ClassNotFoundException {
		this(EntityParser.class.getClassLoader(), className);
	}

	public EntityParser(ClassLoader classLoader, String className) throws ClassNotFoundException {
		this.className = className;
		this.entryClass = classLoader.loadClass(className);
		initEntryInfo();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public <T> T parseToObj(Map<String, Object> cacheMap) {
		if (GU.isNullOrEmpty(cacheMap)) {
			return null;
		}
		Object instanceByDefault = beanInvoker.instanceByDefault();
		cacheMap.entrySet().stream().filter(entry -> beanInvoker.getFieldNames().contains(entry.getKey()))
				.forEach(entry -> {
					Invoker fieldWriter = this.beanInvoker.getFieldWriter(entry.getKey());
					fieldWriter.invoke(instanceByDefault, entry.getValue());
				});
		return (T) instanceByDefault;
	}

	/**
	 * 
	 */
	private void initEntryInfo() {
		this.beanInvoker = BeanInvokeUtils.findBeanMethodInovker(this.entryClass);
	}

}
