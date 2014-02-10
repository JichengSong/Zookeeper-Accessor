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
