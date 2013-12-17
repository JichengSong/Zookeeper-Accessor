/**
 * 
 */
package com.renren.zookeeper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

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

	private Map<EventWatcher, Set<String>> dataWatcherMap = null;
	private Map<EventWatcher, Set<String>> childWatcherMap = null;
	// Management EventWatcher life-cycle
	private Map<Object, EventWatcher> dataOwnerMap = null;
	private Map<Object, EventWatcher> childOwnerMap = null;
	private DaemonThread daemon = null;
	private ZooKeeper zk = null;
	private ZkConfig config = null;

	private Accessor(ZkConfig config) throws InterruptedException, IOException {
		logger.info("Try connect to zk with config : " + config.toString());
		this.config = config;
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

	private void createConnection(ZkConfig config) throws InterruptedException,
			IOException {
		if (zk != null) {
			zk.close();
			zk = null;
		}
		CountDownLatch counter = new CountDownLatch(1);
		zk = new ZooKeeper(config.getHost() + "/" + config.getRoot(),
				config.getSessionTime(), new SessionWatcher(counter));
		counter.await();
		if (config.getUsername() != null && config.getPassword() != null) {
			zk.addAuthInfo("digest", (config.getUsername() + ":" + config
					.getPassword()).getBytes());
		}
	}

	@Override
	protected void finalize() {
		daemon.close();
		try {
			daemon.join();
			zk.close();
			zk = null;
		} catch (InterruptedException e) {
			e.printStackTrace(System.err);
		}
	}

	// Singleton
	private static Accessor instance = null;

	private class DaemonThread extends Thread {
		private boolean running;
		private Accessor parent = null;

		public DaemonThread(Accessor parent) {
			this.parent = parent;
			running = true;
		}

		@Override
		public void run() {
			while (running) {
				int count = 0;
				while (running && count < 3) {
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

		public void close() {
			running = false;
		}

		public void triggerAllWatcher() {
			Set<EventWatcher> keys = dataWatcherMap.keySet();
			Iterator<EventWatcher> it = keys.iterator();
			while (it.hasNext()) {
				Watcher key = it.next();
				Set<String> value = dataWatcherMap.get(key);
				Iterator<String> jt = value.iterator();
				while (jt.hasNext()) {
					WatchedEvent event = new WatchedEvent(
							EventType.NodeDataChanged, KeeperState.Unknown,
							jt.next());
					key.process(event);
				}
			}

			keys = childWatcherMap.keySet();
			it = keys.iterator();
			while (it.hasNext()) {
				EventWatcher key = it.next();
				Set<String> value = childWatcherMap.get(key);
				Iterator<String> jt = value.iterator();
				while (jt.hasNext()) {
					WatchedEvent event = new WatchedEvent(
							EventType.NodeChildrenChanged, KeeperState.Unknown,
							jt.next());
					key.process(event);
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
					logger.warn("Waiting for connected with server");
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
							e.printStackTrace();
							try {
								Thread.sleep(20000);
							} catch (InterruptedException e1) {
								e1.printStackTrace();
							}
						} catch (IOException e) {
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
				} else if (event.getState() == KeeperState.Disconnected) {
					logger.error("Disconnected with zookeeper.");
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

		// 不确定这个函数的重载是否等于 == 的重载
		public boolean equals(EventWatcher eventWatcher) {
			return this.getTriggerWatcher() == eventWatcher.getTriggerWatcher();
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
			if (this.getWatcherType() == null) { // How can it be null?
				return;
			} else if (this.getWatcherType() == WatcherType.Children) {
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
			} else if (this.getWatcherType() == WatcherType.Data) {
				if (dataWatcherMap.containsKey(this)) {
					try {
						Stat stat = null;
						zk.getData(event.getPath(), this, stat);
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
			EventType type = event.getType();
			KeeperState state = event.getState();
			if (event.getType() == EventType.NodeCreated
					|| event.getType() == EventType.NodeDeleted) {
				type = EventType.NodeDataChanged;
				state = KeeperState.Unknown;
			}
			WatchedEvent newEvent = new WatchedEvent(type, state,
					event.getPath());
			this.getTriggerWatcher().process(newEvent);
		}
	}

	public boolean isAvailable() {
		return this.zk.getState().equals(States.CONNECTED);
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
		if (instance.config != config) {
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
		Stat stat = null;
		zk.getData(path, eventWatcher, stat);
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

	public void createEphemerlNode(String path, String value)
			throws IOException, KeeperException, InterruptedException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		zk.create(path, value.getBytes(), Ids.OPEN_ACL_UNSAFE,
				CreateMode.EPHEMERAL);
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

	public String getContent(String path) throws KeeperException,
			InterruptedException, IOException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		Stat stat = null;
		return new String(zk.getData(path, false, stat));
	}

	public void publishService(Publish publish)
			throws OperationNotSupportedException, IOException,
			KeeperException, InterruptedException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		this.createEphemerlNode(publish.getFullPath(), publish.getValue());
		Thread.sleep(1000);
		if (!this.exist(publish.getFullPath())) {
			// if not exist return immediately with KeepException
			throw new IOException("Internal Error");
		}
		this.setDataWatcher(publish, publish.getFullPath(),
				publish.getEphemeralWatcher());
		publish.setStat(this.getStat(publish.getFullPath()));
		publish.setAccessor(this);
	}

	public void setData(String path, String value) throws KeeperException,
			InterruptedException, IOException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		this.zk.setData(path, value.getBytes(), -1);
	}

	public void deleteNode(String path) throws InterruptedException,
			KeeperException, IOException {
		if (!this.isAvailable()) {
			throw new IOException("ZK is not available.");
		}
		zk.delete(path, -1);
	}
	
//	public void subscribeService(Subscribe subscribe) {
//		if (!this.isAvailable()) {
//			throw new IOException("ZK is not available.");
//		}
//	}
}
