/**
 * 
 */
package com.renren.zookeeper;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * @author ZheYuan
 * 
 */
public class ZkConfig {
	private Properties cfg = new Properties();
	private String host = null;
	private String root = null;
	private String username = null;
	private String password = null;
	private int sessionTime;
	private int initTime;

	public int getInitTime() {
		return initTime;
	}

	public void setInitTime(int initTime) {
		this.initTime = initTime;
	}

	public String toString() {
		return "HOST=" + host + " " + "root=" + root + " " + "username="
				+ username + " " + "pasword=" + password + " " + "sessionTime="
				+ sessionTime;
	}

	public ZkConfig() {
		initTime = 10000;
		sessionTime = 10000;
		if (System.getProperty("default.config.path") != null) {
			String path = System.getProperty("default.config.path")
					+ "/zk.conf";
			FileInputStream fileInputStream;
			try {
				fileInputStream = new FileInputStream(path);
				cfg.load(fileInputStream);
				this.setHost(cfg.getProperty("host"));
				this.setInitTime(Integer
						.parseInt(cfg.getProperty("initTime") == null ? "10000"
								: cfg.getProperty("initTime")));
				this.setPassword(cfg.getProperty("password"));
				this.setUsername(cfg.getProperty("username"));
				this.setRoot(cfg.getProperty("root"));
				this.setSessionTime(Integer.parseInt(cfg
						.getProperty("sessionTime") == null ? "10000" : cfg
						.getProperty("sessionTime")));
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}

		}
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		this.host = host;
	}

	public String getRoot() {
		return root;
	}

	public void setRoot(String root) {
		if (root.startsWith("/")) {
			root = root.substring(1);
		}
		this.root = root;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public int getSessionTime() {
		return sessionTime;
	}

	public void setSessionTime(int sessionTime) {
		this.sessionTime = sessionTime;
	}
}
