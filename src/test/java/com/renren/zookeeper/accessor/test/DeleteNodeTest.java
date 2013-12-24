/**
 * 
 */
package com.renren.zookeeper.accessor.test;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;

import com.renren.zookeeper.Accessor;
import com.renren.zookeeper.ZkConfig;

/**
 * @author zhe
 * 
 */
public class DeleteNodeTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ZkConfig config = new ZkConfig();
		config.setHost("xcszookeepertest.n.xiaonei.com");
		config.setHost("localhost");
		config.setSessionTime(4000);
		config.setRoot("xcs-test");
		config.setUsername("test");
		config.setPassword("test");
		Accessor accessor = null;
		try {
			accessor = Accessor.getInstance(config);

		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		while (true) {
			try {
				Thread.sleep(100);
				if (accessor != null) {
					accessor.deleteNode("/test.service/1/0/PublishTestHandle");
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (KeeperException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
