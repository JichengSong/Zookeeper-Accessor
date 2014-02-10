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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.naming.OperationNotSupportedException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.renren.zookeeper.Accessor;
import com.renren.zookeeper.Accessor.WatcherType;
import com.renren.zookeeper.ZkConfig;
import com.renren.zookeeper.accessor.Publish;

public class PublishTest {
	private static class ZookeeperBackgroundServer implements Runnable {

		@Override
		public void run() {
			String[] args = new String[2];
			args[0] = "2181";
			args[1] = "./zkdata";
			QuorumPeerMain.main(args);
		}
	}

	private static ZookeeperBackgroundServer zookeeperBackgroundServer = null;
	private static Accessor accessor = null;
	private static ZooKeeper zooKeeper = null;
	private static ZkConfig config = null;
	private static final String PREFIX_STRING = PublishTest.class.getName();

	private static void recursiveDelete(String path)
			throws InterruptedException, KeeperException, IOException {
		List<String> children = null;
		try {
			children = accessor.getChildren(path);
		} catch (KeeperException.NoNodeException e) {
			// node not exist, ignore.
		}
		if (children != null) {
			Iterator<String> iterator = children.iterator();
			while (iterator.hasNext()) {
				recursiveDelete(path + "/" + iterator.next());
			}
		}
		if (accessor.getStat(path) != null) {
			accessor.deleteNode(path);
		}
	}

	private static void recursiveCreate(String path) throws IOException,
			KeeperException, InterruptedException {
		accessor.createPresistentNode(path, null);
		accessor.createPresistentNode(path + "/1", null);
		accessor.createPresistentNode(path + "/1/0", null);
	}

	@BeforeClass
	public static void init() throws IOException, KeeperException {
		if (zookeeperBackgroundServer == null) {
			zookeeperBackgroundServer = new ZookeeperBackgroundServer();
			Thread zkBackgroundServer = new Thread(zookeeperBackgroundServer);
			zkBackgroundServer.setDaemon(true);
			zkBackgroundServer.start();
		}
		config = new ZkConfig();
		final CountDownLatch countDownLatch = new CountDownLatch(1);
		zooKeeper = new ZooKeeper(config.getHost(), config.getSessionTime(),
				new Watcher() {

					@Override
					public void process(WatchedEvent event) {
						if (event.getType() == EventType.None
								&& event.getState() == KeeperState.SyncConnected) {
							countDownLatch.countDown();
						} else if (event.getType() == EventType.None
								&& event.getState() == KeeperState.Disconnected) {
							System.err
									.println("The connection between zookeeper server and localhost has down.");
						}
					}
				});
		try {
			countDownLatch.await(10, TimeUnit.SECONDS);
		} catch (InterruptedException e1) {
			System.err
					.println("Can't connect zookeeper server, please check 2181 port and config file.");
			e1.printStackTrace();
			throw new IOException("Can't connect zookeeper server.");
		}
		try {
			accessor = Accessor.getInstance(config);
			recursiveDelete('/' + PREFIX_STRING);
			try {
				zooKeeper.delete('/' + config.getRoot(), -1);
			} catch (KeeperException.NoNodeException e1) {
				// node not exist, ignore.
			}
			zooKeeper.create('/' + config.getRoot(), null, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
			recursiveCreate('/' + PREFIX_STRING);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void generalTest() {
	}

	@Test
	public void registerTest() throws IOException, KeeperException {
		try {
			accessor.publishService(new Publish(PREFIX_STRING, "1", "0",
					"register", null));
			Assert.assertNotNull(zooKeeper.exists('/' + config.getRoot() + '/'
					+ PREFIX_STRING + "/1/0/register", false));
			Publish publishHandle = new Publish(PREFIX_STRING, "1", "0",
					"register2", "value".getBytes());
			accessor.publishService(publishHandle);
			Assert.assertNotNull(zooKeeper.exists('/' + config.getRoot()
					+ publishHandle.getFullPath(), false));
			Assert.assertEquals(
					"value",
					new String(zooKeeper.getData('/' + config.getRoot()
							+ publishHandle.getFullPath(), false, new Stat())));
		} catch (OperationNotSupportedException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void registerAndDeleteTest() throws IOException, KeeperException {
		try {
			Publish publishHandle = new Publish(PREFIX_STRING, "1", "0",
					"registerAndDelete", "value".getBytes());
			accessor.publishService(publishHandle);
			Assert.assertNotNull(zooKeeper.exists('/' + config.getRoot()
					+ publishHandle.getFullPath(), false));
			Assert.assertEquals(
					"value",
					new String(zooKeeper.getData('/' + config.getRoot()
							+ publishHandle.getFullPath(), false, new Stat())));
			zooKeeper.delete(
					'/' + config.getRoot() + publishHandle.getFullPath(), -1);
			Thread.sleep(3000);
			Assert.assertNotNull(zooKeeper.exists('/' + config.getRoot()
					+ publishHandle.getFullPath(), false));
			Assert.assertEquals(
					"value",
					new String(zooKeeper.getData('/' + config.getRoot()
							+ publishHandle.getFullPath(), false, new Stat())));
		} catch (OperationNotSupportedException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void dieTest() throws IOException, KeeperException {
		try {
			Publish publishHandle = new Publish(PREFIX_STRING, "1", "0", "die",
					"value".getBytes());
			accessor.publishService(publishHandle);
			Assert.assertNotNull(zooKeeper.exists('/' + config.getRoot()
					+ publishHandle.getFullPath(), false));
			Assert.assertEquals(
					"value",
					new String(zooKeeper.getData('/' + config.getRoot()
							+ publishHandle.getFullPath(), false, new Stat())));
			publishHandle.die();
			Assert.assertNull(zooKeeper.exists('/' + config.getRoot()
					+ publishHandle.getFullPath(), false));
		} catch (OperationNotSupportedException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Test
	public void modifyContentTest() throws IOException, KeeperException {
		try {
			Publish publishHandle = new Publish(PREFIX_STRING, "1", "0",
					"modifyContent", "value".getBytes());
			accessor.publishService(publishHandle);
			Assert.assertNotNull(zooKeeper.exists('/' + config.getRoot()
					+ publishHandle.getFullPath(), false));
			Assert.assertEquals(
					"value",
					new String(zooKeeper.getData('/' + config.getRoot()
							+ publishHandle.getFullPath(), false, new Stat())));
			zooKeeper.setData('/' + config.getRoot()
					+ publishHandle.getFullPath(), "value2".getBytes(), -1);
			Assert.assertNotNull(zooKeeper.exists('/' + config.getRoot()
					+ publishHandle.getFullPath(), false));
			Stat stat = new Stat();
			Assert.assertEquals(
					"value2",
					new String(zooKeeper.getData('/' + config.getRoot()
							+ publishHandle.getFullPath(), false, stat)));
			Assert.assertEquals(1, stat.getVersion());
		} catch (OperationNotSupportedException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
