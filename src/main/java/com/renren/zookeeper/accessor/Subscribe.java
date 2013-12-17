/**
 * 
 */
package com.renren.zookeeper.accessor;

import java.util.List;

import javax.naming.OperationNotSupportedException;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.renren.zookeeper.Accessor;

/**
 * @author ZheYuan
 * 
 */
public abstract class Subscribe {
	private final String serviceId;
	private final String version;
	private final String sharding;
	private Accessor accessor = null;

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

	private class ChildrenWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {
			// TODO Auto-generated method stub

		}

	}

	private class ContentWatcher implements Watcher {

		@Override
		public void process(WatchedEvent event) {

		}

	}

	public abstract void childChanged(List<String> originList,
			List<String> increaceList, List<String> decreaceList);

	public abstract void contentChanged(String path, String oldValue,
			String newValue);
}
