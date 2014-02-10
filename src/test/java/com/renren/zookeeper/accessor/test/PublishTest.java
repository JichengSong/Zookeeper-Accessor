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

import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class PublishTest {
	private class ZookeeperBackgroundServer implements Runnable {

		@Override
		public void run() {
			String [] args = new String[2];
			args[0] = "2181";
			args[1] = "./zkdata";
			QuorumPeerMain.main(args);
		}
	}
	private static ZookeeperBackgroundServer zookeeperBackgroundServer = null;
	
	@BeforeClass
	public static void init() {
		if (zookeeperBackgroundServer == null) {
			Thread zkBackgroundServer = new Thread(zookeeperBackgroundServer);
			zkBackgroundServer.setDaemon(true);
			zkBackgroundServer.start();
		}
	}
	
	@Test
	public void generalTest() {
		Assert.assertEquals(1, 2);
	}
}
