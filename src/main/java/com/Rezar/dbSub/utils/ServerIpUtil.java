package com.Rezar.dbSub.utils;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;

import lombok.extern.slf4j.Slf4j;

/**
 * 
 * @say little Boy, don't be sad.
 * @name Rezar
 * @time 2018年6月15日 下午4:35:39
 * @Desc 些年若许,不负芳华.
 *
 */
@Slf4j
public class ServerIpUtil {

	public static final String SERVER_IP;

	static {
		SERVER_IP = getServerIp();
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
			log.error("error while getServerIp:{}", e);
		}
		return serverIp.matches("\\d+.*") ? serverIp : "127.0.0.1";
	}

	public static String getLocalHostName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			log.error("error:{}", e);
		}
		return "";
	}

	public static void main(String[] args) throws UnknownHostException {
		System.out.print(ServerIpUtil.SERVER_IP);
	}

}
