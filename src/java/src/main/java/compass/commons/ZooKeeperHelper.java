package compass.commons;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZooKeeperHelper implements Watcher {
	private static final Log logger = LogFactory.getLog(ZooKeeperHelper.class
			.getName());

	private String connectString;
	private int sessionTimeout;
	private ZooKeeper keeper;
	private Set<ZooKeeperListener> listeners;
	private Lock lockkeeper;
	private Lock locklisteners;

	public ZooKeeperHelper(String connectString, int sessionTimeout) {
		this.connectString = connectString;
		this.sessionTimeout = sessionTimeout;
		keeper = null;
		listeners = new HashSet<ZooKeeperListener>();
		lockkeeper = new ReentrantLock();
		locklisteners = new ReentrantLock();
	}

	public ZooKeeper getZooKeeper() throws IOException, KeeperException, InterruptedException {
		if (null == keeper) {
			lockkeeper.lock();
			try {
				if (null == keeper) {
					keeper = new ZooKeeper(connectString, sessionTimeout, this);
					keeper.exists("/", true);
				}
			} finally {
				lockkeeper.unlock();
			}
		}
		return keeper;
	}

	public void subscribe(ZooKeeperListener listener) {
		locklisteners.lock();
		try {
			listeners.add(listener);
		} finally {
			locklisteners.unlock();
		}
	}

	@Override
	public void process(WatchedEvent event) {
		NotifyType notifytype = NotifyType.NO;

		if (event.getType() == Event.EventType.None) {
			switch (event.getState()) {
			case SyncConnected:
				notifytype = NotifyType.ALL;
				break;
			case Expired:
				try {
					keeper.close();
				} catch (InterruptedException e) {
					logger.warn(
							"ZooKeeperHelper.process: can't close the keeper",
							e);
				}
				lockkeeper.lock();
				try {
					keeper = null;
				} finally {
					lockkeeper.unlock();
				}
				break;
			default:
				logger.info("ZooKeeperHelper.process: " + event.getType());
			}
		} else if (0 != event.getPath().length()) {
			notifytype = NotifyType.ONE;
		}

		switch (notifytype) {
		case ONE:
			locklisteners.lock();
			try {
				for (ZooKeeperListener listener : listeners) {
					listener.notify(event);
				}
			} finally {
				locklisteners.unlock();
			}
			break;
		case ALL:
			locklisteners.lock();
			try {
				for (ZooKeeperListener listener : listeners) {
					listener.notify();
				}
			} finally {
				locklisteners.unlock();
			}
			break;
		case NO:			
			logger.info("ZooKeeperHelper.process: don't notify");
		default:
			logger.warn("ZooKeeperHelper.process: shouldn't be here");
		}
	}

	enum NotifyType {
		NO, ONE, ALL
	}
}
