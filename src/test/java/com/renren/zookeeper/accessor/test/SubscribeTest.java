/**
 * 
 */
package com.renren.zookeeper.accessor.test;

import java.util.List;

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
		
		public SubscribeInstance(String serviceId, String version, String stat) {
			super(serviceId, version, stat);
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
		try {
			Accessor accessor = Accessor.getInstance(config);
			accessor.subscribeService(subscribe);
			subscribe.die();
			while (true) {
			
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
