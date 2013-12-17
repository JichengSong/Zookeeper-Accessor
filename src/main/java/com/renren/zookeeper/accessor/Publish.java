/**
 * 
 */
package com.renren.zookeeper.accessor;

import java.io.IOException;

import javax.naming.OperationNotSupportedException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import com.renren.zookeeper.Accessor;

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
	private final String serviceId;
	private final String version;
	private final String sharding;
	private final String key;
	private final EphemeralWatcher ephemeralWatcher;
	private String value;
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

	public EphemeralWatcher getEphemeralWatcher() {
		return ephemeralWatcher;
	}

	private class EphemeralWatcher implements Watcher {
		@Override
		public void process(WatchedEvent event) {
			if (event.getType() == EventType.NodeDataChanged) {
				try {
					if (!accessor.exist(event.getPath())) {
						// node disappear
						accessor.createEphemerlNode(getFullPath(), value);
					} else {
						// content changed
						Stat tmpStat = accessor.getStat(getFullPath());
						if (tmpStat.getMzxid() > stat.getMzxid()) {
							stat = tmpStat;
							value = accessor.getContent(getFullPath());
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				} catch (KeeperException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Get full path of this ephemeral node, the value is equal
	 * "/serviceId/version/sharding/key".
	 * 
	 * @return
	 */
	public String getFullPath() {
		return '/' + getServiceId() + '/' + getVersion() + '/' + getSharding()
				+ '/' + getKey();
	}

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

	public Publish(String serviceId, String version, String sharding,
			String key, String value) {
		this.serviceId = serviceId;
		this.version = version;
		this.sharding = sharding;
		this.key = key;
		this.value = value;
		this.ephemeralWatcher = new EphemeralWatcher();
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) throws KeeperException,
			InterruptedException, IOException {
		if (accessor != null) {
			accessor.setData(getFullPath(), value);
		}
		this.value = value;
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

	public String getKey() {
		return key;
	}

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
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
