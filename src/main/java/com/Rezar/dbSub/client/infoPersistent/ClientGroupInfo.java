package com.Rezar.dbSub.client.infoPersistent;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import com.Rezar.dbSub.utils.GU;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年12月5日 下午11:14:39
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public class ClientGroupInfo {

	public static final String CLIENT_GROUP_MARK;
	private static final String simpleFormat = "%s:%s";

	static {
		String serverIp = getServerIp();
		String property = System.getProperty("ClientId");
		if (GU.isNullOrEmpty(property)) {
			property = "default";
		}
		CLIENT_GROUP_MARK = String.format(simpleFormat, serverIp, property);
	}

	@SuppressWarnings("rawtypes")
	private static String getServerIp() {
		String serverIp = null;
		try {
			Enumeration netInterfaces = NetworkInterface.getNetworkInterfaces();
			InetAddress ip = null;
			while (netInterfaces.hasMoreElements()) {
				NetworkInterface ni = (NetworkInterface) netInterfaces.nextElement();
				ip = (InetAddress) ni.getInetAddresses().nextElement();
				serverIp = ip.getHostAddress();
				if (!ip.isSiteLocalAddress() && !ip.isLoopbackAddress() && ip.getHostAddress().indexOf(":") == -1) {
					serverIp = ip.getHostAddress();
					break;
				} else {
					ip = null;
				}
			}
		} catch (Exception e) {
			log.error("error take ip:{}", e);
		}
		if (serverIp == null || !serverIp.matches("(\\d+\\.?){4}")) {
			return "127.0.0.1";
		}
		return serverIp;
	}

	public static void main(String[] args) {
		System.out.println(getServerIp());
	}

}
