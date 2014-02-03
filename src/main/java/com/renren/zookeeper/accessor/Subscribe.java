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
package com.renren.zookeeper.accessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.OperationNotSupportedException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import com.renren.zookeeper.Accessor;
import com.renren.zookeeper.Pair;

/**
 * @author ZheYuan
 * 
 */
public abstract class Subscribe {
	private static Logger logger = LogManager.getLogger(Subscribe.class
			.getName());
	private final String serviceId;
	private final String version;
	private final String sharding;
	private Accessor accessor = null;
	private final ChildrenWatcher childrenWatcher;
	private List<String> endpoints = null;
	private Map<String, Endpoint> endpointStatMap = null;

	public void initData() throws RuntimeException, KeeperException,
			InterruptedException, IOException {
		if (this.getAccessor() != null) {
			endpoints = this.getAccessor().getChildren(this.getFullPath());
			Collections.sort(endpoints);
			this.childChanged(null, endpoints, null);
			Iterator<String> endpoint = endpoints.iterator();
			while (endpoint.hasNext()) {
				String node = endpoint.next();
				Pair<byte[], Stat> pair = new Pair<byte[], Stat>(
						new byte[1024 * 1024], new Stat());
				if (accessor.getContentAndStat(this.getFullPath() + '/' + node,
						pair)) { // node exist
					endpointStatMap.put(node, new Endpoint(node, pair.first,
							pair.second));
					accessor.setDataWatcher(endpointStatMap.get(node),
							this.getFullPath() + '/' + node, endpointStatMap
									.get(node).getContentWatcher());
				}
			}
		} else {
			throw new RuntimeException("Accessor have not set.");
		}
	}

	private class Endpoint {
		private final String node;
		private Stat stat;
		private byte[] value;
		private final ContentWatcher contentWatcher;

		/**
		 * @return the contentWatcher
		 */
		public ContentWatcher getContentWatcher() {
			return contentWatcher;
		}

		public Endpoint(String node, byte[] value, Stat stat) {
			this.node = node;
			this.value = value;
			this.stat = stat;
			contentWatcher = new ContentWatcher();
		}

		/**
		 * @return the stat
		 */
		public Stat getStat() {
			return stat;
		}

		/**
		 * @param stat
		 *            the stat to set
		 */
		public void setStat(Stat stat) {
			this.stat = stat;
		}

		/**
		 * @return the value
		 */
		public byte[] getValue() {
			return value;
		}

		/**
		 * @param value
		 *            the value to set
		 */
		public void setValue(byte[] value) {
			this.value = value;
		}

		/**
		 * @return the node
		 */
		public String getNode() {
			return node;
		}
	}

	public synchronized void setAccessor(Accessor accessor)
			throws OperationNotSupportedException {
		if (this.accessor == null) {
			this.accessor = accessor;
		} else {
			throw new OperationNotSupportedException("");
		}
	}

	public String getServiceId() {
		return serviceId;
	}

	public String getVersion() {
		return version;
	}

	public String getSharding() {
		return sharding;
	}

	public Accessor getAccessor() {
		return accessor;
	}

	public Subscribe(String serviceId, String version, String sharding) {
		this.serviceId = serviceId;
		this.version = version;
		this.sharding = sharding;
		childrenWatcher = new ChildrenWatcher();
		endpointStatMap = new ConcurrentHashMap<String, Endpoint>();
	}

	/**
	 * Get full path of this ephemeral node, the value is equal
	 * "/serviceId/version/sharding".
	 * 
	 * @return
	 */
	public String getFullPath() {
		return '/' + getServiceId() + '/' + getVersion() + '/' + getSharding();
	}

	/**
	 * @return the childrenWatcher
	 */
	public ChildrenWatcher getChildrenWatcher() {
		return childrenWatcher;
	}

	private class ChildrenWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			if (event.getType() == EventType.NodeChildrenChanged) {
				try {
					List<String> removeList = new ArrayList<String>();
					List<String> addList = new ArrayList<String>();
					List<String> oldChildrenList = endpoints;
					List<String> newChildrenList = accessor.getChildren(event
							.getPath());
					Collections.sort(newChildrenList);
					endpoints = newChildrenList;
					int oldPos = 0, newPos = 0;
					while (oldPos < oldChildrenList.size()
							&& newPos < newChildrenList.size()) {
						String oldChild = oldChildrenList.get(oldPos);
						String newChild = newChildrenList.get(newPos);
						if (newChild.compareTo(oldChild) < 0) { // add
							Pair<byte[], Stat> pair = new Pair<byte[], Stat>(
									new byte[1024 * 1024], new Stat());
							if (accessor.getContentAndStat(getFullPath() + '/'
									+ newChild, pair)) {
								endpointStatMap.put(newChild, new Endpoint(
										newChild, pair.first, pair.second));
								accessor.setDataWatcher(endpointStatMap
										.get(newChild), getFullPath() + '/'
										+ newChild,
										endpointStatMap.get(newChild)
												.getContentWatcher());
								addList.add(newChild);
							}
							newPos++;
						} else if (newChild.compareTo(oldChild) > 0) { // remove
							accessor.delDataWatcher(endpointStatMap
									.get(oldChild));
							endpointStatMap.remove(oldChild);
							removeList.add(oldChild);
							oldPos++;
						} else { // equal
							Endpoint oldEndpoint = endpointStatMap
									.get(oldChild);
							Stat oldStat = oldEndpoint.getStat();
							Pair<byte[], Stat> pair = new Pair<byte[], Stat>(
									new byte[1024 * 1024], new Stat());
							if (accessor.getContentAndStat(getFullPath() + '/'
									+ newChild, pair)) {
								if (pair.second.compareTo(oldStat) > 0) { // remove-add
																			// or
																			// data
																			// watcher
																			// loss
									oldEndpoint.setStat(pair.second);
									oldEndpoint.setValue(pair.first);
									addList.add(newChild);
									removeList.add(oldChild);
								}
							} else {
								accessor.delDataWatcher(endpointStatMap
										.get(oldChild));
								endpointStatMap.remove(oldChild);
								removeList.add(oldChild);
							}
							oldPos++;
							newPos++;
						}
					}

					if (newPos < newChildrenList.size()) {
						while (newPos < newChildrenList.size()) {
							String newChild = newChildrenList.get(newPos);
							Pair<byte[], Stat> pair = new Pair<byte[], Stat>(
									new byte[1024 * 1024], new Stat());
							if (accessor.getContentAndStat(getFullPath() + '/'
									+ newChild, pair)) {
								endpointStatMap.put(newChild, new Endpoint(
										newChild, pair.first, pair.second));
								accessor.setDataWatcher(endpointStatMap
										.get(newChild), getFullPath() + '/'
										+ newChild,
										endpointStatMap.get(newChild)
												.getContentWatcher());
								addList.add(newChild);
							}
							newPos++;
						}
					} else if (oldPos < oldChildrenList.size()) {
						while (oldPos < oldChildrenList.size()) {
							String oldChild = oldChildrenList.get(oldPos);
							accessor.delDataWatcher(endpointStatMap
									.get(oldChild));
							endpointStatMap.remove(oldChild);
							removeList.add(oldChild);
							oldPos++;
						}
					}
					childChanged(oldChildrenList, addList, removeList);
				} catch (Exception e) {
					logger.error("When children callback process, "
							+ e.getMessage());
					e.printStackTrace();
				}

			}

		}
	}

	private class ContentWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			if (event.getType() == EventType.NodeDataChanged) {
				try {
					String endpoint = event.getPath().substring(
							getFullPath().length() + 1);
					Endpoint oldEndpoint = endpointStatMap.get(endpoint);
					Pair<byte[], Stat> pair = new Pair<byte[], Stat>(
							new byte[1024 * 1024], new Stat());
					byte[] oldValue = oldEndpoint.getValue();
					if (accessor.getContentAndStat(event.getPath(), pair)) {
						if (pair.second.compareTo(oldEndpoint.getStat()) > 0) { // new
																				// value
																				// instead
																				// of
																				// old
																				// value
							oldEndpoint.setStat(pair.second);
							oldEndpoint.setValue(pair.first);
							contentChanged(event.getPath(), oldValue,
									pair.first);
						}
					}
				} catch (Exception e) {
					logger.error("When content callback process, "
							+ e.getMessage());
					e.printStackTrace();
				}
			}
		}

	}

	public void die() {
		if (accessor != null) {
			accessor.delChildrenWatcher(this);
			accessor.delDataWatcher(this);
		}
	}
	
	@Override
	protected void finalize() {
		try {
			die();
		} catch (Exception e) {
			logger.error("When subscribe destory, " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	public byte[] getContent(String child) throws KeeperException,
			InterruptedException, IOException {
		return accessor.getContent(getFullPath() + '/' + child);
	}

	public abstract void childChanged(List<String> originList,
			List<String> increaceList, List<String> decreaceList);

	public abstract void contentChanged(String path, byte[] oldValue,
			byte[] newValue);
}
