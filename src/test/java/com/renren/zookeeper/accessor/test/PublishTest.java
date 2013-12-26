/**
 * 
 */
package com.renren.zookeeper.accessor.test;

import com.renren.zookeeper.Accessor;
import com.renren.zookeeper.ZkConfig;
import com.renren.zookeeper.accessor.Publish;

/**
 * @author ZheYuan
 * 
 */
public class PublishTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		ZkConfig config = new ZkConfig();
		try {
			Accessor accessor = Accessor.getInstance(config);
			accessor.publishService(new Publish("test.service", "1", "0",
					"PublishTest", null));
			
			Publish publish = new Publish("test.service", "1", "0",
					"PublishTestHandle", "127.0.0.1".getBytes());
			accessor.publishService(publish);
			//publish.die();
			while (true) {
				System.out.println(System.currentTimeMillis() + ":" + publish.getFullPath() + "@"
						+ new String(publish.getValue()));
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
