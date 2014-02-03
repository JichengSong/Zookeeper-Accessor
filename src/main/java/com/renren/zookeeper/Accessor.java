/**
 * 
 */
package com.renren.zookeeper;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.naming.OperationNotSupportedException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.renren.zookeeper.accessor.Publish;
import com.renren.zookeeper.accessor.Subscribe;

/**
 * @author ZheYuan
 * 
 */
public class Accessor {
	private static Logger logger = LogManager.getLogger(Accessor.class
			.getName());
	private volatile boolean closed;
	private Map<EventWatcher, Set<String>> dataWatcherMap = null;
	private Map<EventWatcher, Set<String>> childWatcherMap = null;
	// Management EventWatcher life-cycle
	private Map<Object, EventWatcher> dataOwnerMap = null;
	private Map<Object, EventWatcher> childOwnerMap = null;
	private DaemonThread daemon = null;
	private ZooKeeper zk = null;
	private ZkConfig config = null;
	private final int daemonSleepTime = 300; // 5min

	private Accessor(ZkConfig config) throws InterruptedException, IOException {
		logger.info("Try connect to zk with config : " + config.toString());
		this.config = config;
		this.closed = false;
		dataWatcherMap = new ConcurrentHashMap<EventWatcher, Set<String>>();
		childWatcherMap = new ConcurrentHashMap<EventWatcher, Set<String>>();
		dataOwnerMap = new ConcurrentHashMap<Object, Accessor.EventWatcher>();
		childOwnerMap = new ConcurrentHashMap<Object, Accessor.EventWatcher>();
		createConnection(config);
		if (daemon == null) {
			daemon = new DaemonThread(this);
			daemon.setDaemon(true);
			daemon.start();
		}
	}

	private void close() {
		this.closed = true;
		daemon.close();
		try {
			daemon.join();
			if (zk != null) {
				zk.close();
				zk = null;
			}
		} catch (InterruptedException e) {
			e.printStackTrace(System.err);
		}
	}
	
	private void createConnection(ZkConfig config) throws InterruptedException,
			IOException {
		if (zk != null) {
			zk.close();
			zk = null;
		}
		CountDownLatch counter = new CountDownLatch(1);
		Thread.sleep(new Random().nextInt(10) * 1000);
		zk = new ZooKeeper(config.getHost() + "/" + config.getRoot(),
				config.getSessionTime(), new SessionWatcher(counter));
		logger.warn("Waiting for connected with server");
		if (!counter.await(config.getInitTime(), TimeUnit.MILLISECONDS)) {
			zk.close();
			zk = null;
			throw new IOException("Can't connect zookeeper.");
		}
		if (config.getUsername() != null && config.getPassword() != null) {
			zk.addAuthInfo("digest", (config.getUsername() + ":" + config
					.getPassword()).getBytes());
		}
	}

	@Override
	protected void finalize() {
		if (!closed) {
			this.close();
		}
	}

	// Singleton
	private static Accessor instance = null;

	private class DaemonThread extends Thread {
		volatile private boolean running;
		private Accessor parent = null;

		public DaemonThread(Accessor parent) {
			this.parent = parent;
			running = true;
		}

		@Override
		public void run() {
			while (running) {
				int count = 0;
				while (running && count < daemonSleepTime) {
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					count++;
				}
				if (!running) { // quick exit
					break;
				}
				if (parent.isAvailable()) {
					triggerAllWatcher();
				}
			}
		}

		private void close() {
			running = false;
		}

		@SuppressWarnings("deprecation")
		public void triggerAllWatcher() {
			synchronized (parent.zk) {
				Iterator<Map.Entry<EventWatcher, Set<String>>> iterator = dataWatcherMap
						.entrySet().iterator();
				Map.Entry<EventWatcher, Set<String>> entry = null;
				while (iterator.hasNext()) {
					entry = iterator.next();
					Watcher keyWatcher = entry.getKey();
					for (String item : entry.getValue()) {
						WatchedEvent event = new WatchedEvent(
								EventType.NodeDataChanged, KeeperState.Unknown,
								item);
						keyWatcher.process(event);
					}
				}

				iterator = childWatcherMap.entrySet().iterator();
				while (iterator.hasNext()) {
					entry = iterator.next();
					Watcher keyWatcher = entry.getKey();
					for (String item : entry.getValue()) {
						WatchedEvent event = new WatchedEvent(
								EventType.NodeChildrenChanged,
								KeeperState.Unknown, item);
						keyWatcher.process(event);
					}
				}
			}
		}

	}

	private class SessionWatcher implements Watcher {
		private CountDownLatch counter;

		public SessionWatcher(CountDownLatch counter) {
			this.counter = counter;
		}

		@Override
		public void process(WatchedEvent event) {
			if (event.getType() == EventType.None) {
				if (event.getState() == KeeperState.SyncConnected) {
					counter.countDown();
					logger.warn("Connected with server");
				} else if (event.getState() == KeeperState.Expired) {
					logger.warn("Connection not established, re-connect.");
					boolean connected = false;
					while (!connected) {
						try {
							createConnection(config);
							daemon.triggerAllWatcher();
							connected = true;
						} catch (InterruptedException e) {
							logger.error("Fail to re-connect zookeeper");
							e.printStackTrace();
							try {
								Thread.sleep(20000);
							} catch (InterruptedException e1) {
								e1.printStackTrace();
							}
						} catch (IOException e) {
							logger.error("Fail to re-connect zookeeper");
							e.printStackTrace(System.err);
							try {
								Thread.sleep(20000);
							} catch (InterruptedException e1) {
								e1.printStackTrace();
							}
						}
					}
				} else if (event.getState() == KeeperState.AuthFailed) {
					logger.error("Auth failed! Check username and password are correct and contact adminstrator.");
					System.exit(1);
				} else if (event.getState() == KeeperState.Disconnected) {
					logger.error("Disconnected with zookeeper.");
					boolean connected = false;
					while (!connected) {
						try {
							createConnection(config);
							daemon.triggerAllWatcher();
							connected = true;
						} catch (InterruptedException e) {
							logger.error("Fail to re-connect zookeeper");
							e.printStackTrace();
							try {
								Thread.sleep(20000);
							} catch (InterruptedException e1) {
								e1.printStackTrace();
							}
						} catch (IOException e) {
							logger.error("Fail to re-connect zookeeper");
							e.printStackTrace(System.err);
							try {
								Thread.sleep(20000);
							} catch (InterruptedException e1) {
								e1.printStackTrace();
							}
						}
					}
				}
			}
		}
	}

	public enum WatcherType {
		Children(1), Data(0);
		private final int intValue;

		private WatcherType(int intValue) {
			this.intValue = intValue;
		}

		public int getIntValue() {
			return intValue;
		}

		public static WatcherType fromInt(int intValue) {
			switch (intValue) {
			case 0:
				return WatcherType.Data;
			case 1:
				return WatcherType.Children;

			default:
				throw new RuntimeException(
						"Invalid integer value for conversion to KeeperState");
			}
		}
	}

	private class EventWatcher implements Watcher {
		private final Watcher triggerWatcher;
		private final WatcherType watcherType;

		public Watcher getTriggerWatcher() {
			return triggerWatcher;
		}
		
		@Override
		public boolean equals(Object eventWatcher) {
			return this.getTriggerWatcher().equals(((EventWatcher) eventWatcher).getTriggerWatcher());
		}

		public EventWatcher(Watcher triggerWatcher, WatcherType watcherType) {
			this.triggerWatcher = triggerWatcher;
			this.watcherType = watcherType;
		}

		public WatcherType getWatcherType() {
			return watcherType;
		}

		@Override
		public void process(WatchedEvent event) {
			synchronized (zk) {
				if (this.getWatcherType() == null) { // How can it be null?
					return;
				} else if (this.getWatcherType().equals(WatcherType.Children)) {
					if (childWatcherMap.containsKey(this)) {
						try {
							zk.getChildren(event.getPath(), this);
						} catch (KeeperException e) {
							logger.error("Event Children Watcher path = "
									+ event.getPath());
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					} else {
						return;
					}
				} else if (this.getWatcherType().equals(WatcherType.Data)) {
					if (dataWatcherMap.containsKey(this)) {
						try {
							zk.exists(event.getPath(), this);
						} catch (KeeperException e) {
							logger.error("Event Data Watcher path = "
									+ event.getPath());
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
					} else {
						return;
					}
				}
				this.getTriggerWatcher().process(event);
			}
		}
	}

	public boolean isAvailable() {
		return (!isClosed()) && this.zk.getState().equals(States.CONNECTED);
	}

	public boolean isClosed() {
		return this.closed;
	}
	
	public synchronized static Accessor getInstance(ZkConfig config)
			throws InterruptedException, IOException {
		if (config == null) {
			return instance;
		}
		if (instance == null) {
			instance = new Accessor(config);
			return instance;
		}
		if (!instance.config.equals(config)) {
			instance = new Accessor(config);
			return instance;
		}
		// config duplicate, no operation
		return instance;
	}

	public void setChildrenWatcher(Object owner, String path, Watcher watcher)
			throws IOException, KeeperException, InterruptedException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		EventWatcher eventWatcher = new EventWatcher(watcher,
				WatcherType.Children);
		zk.getChildren(path, eventWatcher);
		childOwnerMap.put(owner, eventWatcher);
		if (!childWatcherMap.containsKey(eventWatcher)) {
			childWatcherMap.put(eventWatcher, new TreeSet<String>());
		}
		childWatcherMap.get(eventWatcher).add(path);
	}

	public void setDataWatcher(Object owner, String path, Watcher watcher)
			throws KeeperException, InterruptedException, IOException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		EventWatcher eventWatcher = new EventWatcher(watcher, WatcherType.Data);
		zk.exists(path, eventWatcher);
		dataOwnerMap.put(owner, eventWatcher);
		if (!dataWatcherMap.containsKey(eventWatcher)) {
			dataWatcherMap.put(eventWatcher, new TreeSet<String>());
		}
		dataWatcherMap.get(eventWatcher).add(path);
	}

	public void delChildrenWatcher(Object owner) {
		if (childOwnerMap.containsKey(owner)) {
			Watcher eventWatcher = childOwnerMap.get(owner);
			childWatcherMap.remove(eventWatcher);
		}
		childOwnerMap.remove(owner);
	}

	public void delDataWatcher(Object owner) {
		if (dataOwnerMap.containsKey(owner)) {
			Watcher eventWatcher = dataOwnerMap.get(owner);
			dataWatcherMap.remove(eventWatcher);
		}
		dataOwnerMap.remove(owner);
	}

	public void createEphemerlNode(String path, byte[] value)
			throws IOException, KeeperException, InterruptedException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		if (value != null && value.length > 1024 * 1024) { // 1M
			throw new IOException("Value too large.");
		}
		while (true) {
			try {
				zk.create(path, value, Ids.OPEN_ACL_UNSAFE,
						CreateMode.EPHEMERAL);
				break;
			} catch (KeeperException.ConnectionLossException e) {
			} catch (KeeperException.NodeExistsException e) {
			}
		}
	}

	public boolean exist(String path) throws IOException, KeeperException,
			InterruptedException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		Stat stat = zk.exists(path, false);
		return (stat != null);
	}

	public Stat getStat(String path) throws IOException, KeeperException,
			InterruptedException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		Stat stat = zk.exists(path, false);
		return stat;
	}

	public byte[] getContent(String path) throws KeeperException,
			KeeperException.NoNodeException, InterruptedException, IOException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		return zk.getData(path, false, null);
	}

	public void publishService(Publish publish)
			throws OperationNotSupportedException, IOException,
			KeeperException, InterruptedException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		publish.setAccessor(this);
		while (true) {
			try {
				this.createEphemerlNode(publish.getFullPath(),
						publish.getValue());
				Thread.sleep(1000);
				break;
			} catch (KeeperException.NodeExistsException e) {
				logger.warn("Node exists with path " + publish.getFullPath()
						+ new String(publish.getValue()) + ". Sleep 10s and retry.");
				Thread.sleep(10000);
			}
		}
		if (!this.exist(publish.getFullPath())) {
			// if not exist return immediately with IOException
			publish.die();
			throw new IOException("Internal Error");
		}
		this.setDataWatcher(publish, publish.getFullPath(),
				publish.getEphemeralWatcher());
		publish.setStat(this.getStat(publish.getFullPath()));
	}

	public void setContent(String path, byte[] value) throws KeeperException,
			KeeperException.NoNodeException, InterruptedException, IOException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		if (value != null && value.length > 1024 * 1024) { // 1M
			throw new IOException("Value too large.");
		}
		this.zk.setData(path, value, -1);
	}

	public void deleteNode(String path) throws InterruptedException,
			KeeperException, IOException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		zk.delete(path, -1);
	}

	public void subscribeService(Subscribe subscribe) throws KeeperException,
			InterruptedException, IOException, OperationNotSupportedException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		subscribe.setAccessor(this);
		/**
		 * 先initData后setWatcher，有一定的概率导致数据不一致，可追回。
		 * Zookeeper本身就不是一个强一致性系统，只能保证最终一致。
		 */
		subscribe.initData();
		this.setChildrenWatcher(subscribe, subscribe.getFullPath(),
				subscribe.getChildrenWatcher());
	}

	public List<String> getChildren(String path) throws KeeperException,
			InterruptedException, IOException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		return zk.getChildren(path, false);
	}

	public boolean getContentAndStat(String path, Pair<byte[], Stat> ret)
			throws KeeperException, InterruptedException, IOException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		try {
			byte[] value = zk.getData(path, false, ret.second);
			ret.first = value;
		} catch (KeeperException.NoNodeException e) {
			ret.first = null;
			ret.second = null;
			return false;
		}
		return true;
	}
}
