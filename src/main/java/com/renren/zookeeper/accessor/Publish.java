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
 * <p>
 * Class Publish
 * </p>
 * 
 * @author ZheYuan
 * @description Class Publish is used to publish a service to zk. It supports
 *              four level directory in zk like
 *              "/serviceId/version/sharding/key" with value. example : Accessor
 *              accessor = Accessor.getInstance(null);
 *              accessor.publishService(new Publish(serviceId, version,
 *              sharding, key, end));
 */
public final class Publish {
	private static Logger logger = LogManager
			.getLogger(Publish.class.getName());

	private final String serviceId;
	private final String version;
	private final String sharding;
	private final String key;
	private final EphemeralWatcher ephemeralWatcher;
	private byte[] value;
	private Accessor accessor = null;
	private Stat stat;

	public Stat getStat() {
		return stat;
	}

	/**
	 * Internal method, I havn't think a good way to let it invisible.
	 * 
	 * @param stat
	 */
	public void setStat(Stat stat) {
		this.stat = stat;
	}
	
	/**
	 * Internal method, I havn't think a good way to let it invisible.
	 * 
	 * @return ephemeral watcher
	 */
	public EphemeralWatcher getEphemeralWatcher() {
		return ephemeralWatcher;
	}

	private class EphemeralWatcher implements Watcher {
		private final Publish publish;

		public EphemeralWatcher(Publish publish) {
			this.publish = publish;
		}

		@Override
		public void process(WatchedEvent event) {
			if (event.getType() == EventType.NodeDataChanged
					|| event.getType() == EventType.NodeDeleted
					|| event.getType() == EventType.NodeCreated) {
				try {
					Pair<byte[], Stat> pair = new Pair<byte[], Stat>(
							new byte[1024 * 1024], new Stat());
					if (accessor.getContentAndStat(event.getPath(), pair)) {
						// content changed
						if (pair.second.compareTo(stat) > 0) {
							stat = pair.second;
							value = pair.first;
						} // else ignore this event
					} else {
						// node disappear
						accessor.createEphemerlNode(getFullPath(), value);
						accessor.setDataWatcher(this.publish, getFullPath(),
								this);
					}
				} catch (Exception e) {
					logger.error("When callback process, " + e.getMessage());
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Get full path of this ephemeral node, the value is equal
	 * "/serviceId/version/sharding/key".
	 * 
	 * @return "/serviceId/version/sharding/key"
	 */
	public String getFullPath() {
		return '/' + getServiceId() + '/' + getVersion() + '/' + getSharding()
				+ '/' + getKey();
	}

	/**
	 * If this instance has published then get the accessor of this.
	 * 
	 * @return null or accessor.
	 */
	public Accessor getAccessor() {
		return accessor;
	}

	/**
	 * A Pubish instance only publish once.
	 * 
	 * @param accessor
	 * @throws OperationNotSupportedException
	 */
	public synchronized void setAccessor(Accessor accessor)
			throws OperationNotSupportedException {
		if (this.accessor == null) {
			this.accessor = accessor;
		} else {
			throw new OperationNotSupportedException("");
		}
	}

	/**
	 * Construction Function.
	 * Path like "/serviceId/version/sharding/key".
	 * 
	 * @param serviceId
	 * @param version
	 * @param sharding
	 * @param key
	 * @param value
	 */
	public Publish(String serviceId, String version, String sharding,
			String key, byte[] value) {
		this.serviceId = serviceId;
		this.version = version;
		this.sharding = sharding;
		this.key = key;
		this.value = value;
		this.ephemeralWatcher = new EphemeralWatcher(this);
	}

	/**
	 * Value will be saved on zookeeper with auto synchronized.
	 * 
	 * @return Get publish's value.
	 */
	public byte[] getValue() {
		return value;
	}

	/**
	 * Set local value and auto synchronize to zookeeper.
	 * 
	 * @param value
	 * @throws KeeperException
	 * @throws InterruptedException
	 * @throws IOException
	 */
	public void setValue(byte[] value) throws KeeperException,
			InterruptedException, IOException {
		if (accessor != null) {
			accessor.setContent(getFullPath(), value);
		}
		this.value = value;
	}

	/**
	 * Get first level directory service id.
	 * 
	 * @return serviceId
	 */
	public String getServiceId() {
		return serviceId;
	}
	
	/**
	 * Get second level directory version.
	 * @return version
	 */
	public String getVersion() {
		return version;
	}

	/**
	 * Get third level directory sharding.
	 * 
	 * @return sharding
	 */
	
	public String getSharding() {
		return sharding;
	}

	/**
	 * Get endpoint.
	 * 
	 * @return endpoint
	 */
	
	public String getKey() {
		return key;
	}

	/**
	 * Remove publish in zookeeper forever.
	 * 
	 * @throws InterruptedException
	 * @throws KeeperException
	 * @throws IOException
	 */
	public void die() throws InterruptedException, KeeperException, IOException {
		if (accessor != null) {
			accessor.delChildrenWatcher(this);
			accessor.delDataWatcher(this);
			accessor.deleteNode(getFullPath());
		}
	}

	@Override
	protected void finalize() {
		try {
			die();
		} catch (Exception e) {
			logger.error("When publish destory, " + e.getMessage());
			e.printStackTrace();
		}
	}
}
