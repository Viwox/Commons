package compass.commons;

import org.apache.zookeeper.WatchedEvent;

public interface ZooKeeperListener {
	public void notify(WatchedEvent event);
	public void notifyAll(WatchedEvent event);
}
