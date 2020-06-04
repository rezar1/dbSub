package com.Rezar.dbSub.server.configParser;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.dom4j.Attribute;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.Rezar.dbSub.base.dbInfo.SubTableInfoForServer;
import com.Rezar.dbSub.server.dbInfo.DBInitInfo;
import com.Rezar.dbSub.server.dbInfo.DatabaseInitInfo;
import com.Rezar.dbSub.utils.beanUtil.BeanInvokeUtils;
import com.Rezar.dbSub.utils.beanUtil.BeanMethodInvoke;
import com.Rezar.dbSub.utils.beanUtil.Invoker;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年11月24日 下午5:45:27
 * @Desc 些年若许,不负芳华.
 *
 */
public class ConfigLoader {

	public static List<DatabaseInitInfo> parserConfigFromClasspath(String configPath) throws Exception {
		InputStream is = ConfigLoader.class.getClassLoader().getResourceAsStream(configPath);
		return parseBinaryConfig(is);
	}

	/**
	 * @param is
	 * @return
	 * @throws DocumentException
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private static List<DatabaseInitInfo> parseBinaryConfig(InputStream is) throws DocumentException, IOException {
		List<DatabaseInitInfo> databaseInitInfoList;
		try {
			SAXReader reader = new SAXReader();
			Document doc = reader.read(is);
			Element root = doc.getRootElement();
			Iterator<Element> it = root.elementIterator();
			databaseInitInfoList = new ArrayList<>();
			while (it.hasNext()) {
				Element e = it.next();// 获取子元素
				String eleName = e.getName();
				if (eleName.equals("database")) {
					databaseInitInfoList.add(parseDatabaseEle(e));
				}
			}
		} finally {
			is.close();
		}
		return databaseInitInfoList;
	}

	public static List<DatabaseInitInfo> parserConfigFromFile(String configPath) throws Exception {
		InputStream is = new FileInputStream(new File(configPath));
		return parseBinaryConfig(is);
	}

	/**
	 * @param e
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private static DatabaseInitInfo parseDatabaseEle(Element databaseEle) {
		DatabaseInitInfo initInfo = new DatabaseInitInfo();
		BeanMethodInvoke<DatabaseInitInfo> findBeanMethodInovker = BeanInvokeUtils
				.findBeanMethodInovker(DatabaseInitInfo.class);
		BeanMethodInvoke<SubTableInfoForServer> subTableInfoBeanInvoker = BeanInvokeUtils
				.findBeanMethodInovker(SubTableInfoForServer.class);
		Iterator<Attribute> attributeIterator = databaseEle.attributeIterator();
		for (; attributeIterator.hasNext();) {
			Attribute next = attributeIterator.next();
			findBeanMethodInovker.getFieldWriter(next.getName()).invoke(initInfo, next.getText());
		}
		List<Element> dbList = databaseEle.elements("db");
		for (Element dbEle : dbList) {
			DBInitInfo dbInitInfo = new DBInitInfo();
			Attribute attribute = dbEle.attribute("name");
			dbInitInfo.setDb(attribute.getText());
			List<Element> subTableEleList = dbEle.elements("table");
			for (Element subTable : subTableEleList) {
				SubTableInfoForServer instanceByDefault = subTableInfoBeanInvoker.instanceByDefault();
				Iterator<Attribute> subTableAttribute = subTable.attributeIterator();
				for (; subTableAttribute.hasNext();) {
					Attribute next = subTableAttribute.next();
					Invoker<SubTableInfoForServer> fieldWriter = subTableInfoBeanInvoker.getFieldWriter(next.getName());
					if (fieldWriter != null) {
						fieldWriter.invoke(instanceByDefault, next.getText());
					}
				}
				dbInitInfo.getSubTableInfo().add(instanceByDefault);
			}
			initInfo.getSubDBInitInfos().add(dbInitInfo);
		}
		return initInfo;
	}

}
