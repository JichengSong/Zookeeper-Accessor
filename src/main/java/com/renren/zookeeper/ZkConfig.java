/**
    Copyright (C) 2013-2014 Zhe Yuan
    
    This file is part of Zookeeper-Accessor.
    
    Zookeeper-Accessor is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

	@Override
	public boolean equals(Object o) {
		ZkConfig config = (ZkConfig) o;
		return this.getHost().equals(config.getHost())
				&& this.getRoot().equals(config.getRoot())
				&& this.getUsername().equals(config.getUsername())
				&& this.getPassword().equals(config.getPassword())
				&& this.getInitTime() == config.getInitTime()
				&& this.getSessionTime() == config.getSessionTime();
	}

	@Override
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
