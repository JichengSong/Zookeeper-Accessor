/**
 * 
 */
package com.renren.zookeeper.accessor.test;

import java.io.IOException;
import java.util.List;

import javax.naming.OperationNotSupportedException;

import org.apache.zookeeper.KeeperException;

import com.renren.zookeeper.Accessor;
import com.renren.zookeeper.ZkConfig;
import com.renren.zookeeper.accessor.Subscribe;

/**
 * @author ZheYuan
 * 
 */
public class SubscribeTest {

	private static class SubscribeInstance extends Subscribe {

		public SubscribeInstance() {
			super("test.service", "1", "0");
		}
		
		@Override
		public void childChanged(List<String> originList,
				List<String> increaceList, List<String> decreaceList) {
			System.out.println("Origin : " + originList);
			System.out.println("Increace : " + increaceList);
			System.out.println("Decreace : " + decreaceList);
		}

		@Override
		public void contentChanged(String path, byte[] oldValue, byte[] newValue) {
			System.out.println("Path = " + path + " oldvalue = " + new String(oldValue)
					+ " newvalue = " + new String(newValue));
		}

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SubscribeInstance subscribe = new SubscribeInstance();
		ZkConfig config = new ZkConfig();
		config.setHost("xcszookeepertest.n.xiaonei.com:2181");
		config.setHost("localhost");
		config.setSessionTime(5000);
		config.setRoot("xcs-test");
		config.setUsername("test");
		config.setPassword("test");
		try {
			Accessor accessor = Accessor.getInstance(config);
			accessor.subscribeService(subscribe);
			while (true) {
			
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
