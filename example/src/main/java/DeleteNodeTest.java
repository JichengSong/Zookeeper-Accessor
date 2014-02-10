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
