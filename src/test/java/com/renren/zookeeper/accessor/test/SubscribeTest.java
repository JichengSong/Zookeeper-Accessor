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
