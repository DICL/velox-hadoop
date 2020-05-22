package org.dicl.velox.mapreduce;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Transaction;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class LeanSession {
	private static final Log LOG = LogFactory.getLog(LeanSession.class);
	private ZooKeeper zk;
	private String fullPath;
	CountDownLatch connSignal = new CountDownLatch(0);

	public LeanSession(String addr, String jobId, int timeout) {
		fullPath = "/chunks/" + jobId;

		try {
			zk = new ZooKeeper(addr, timeout, new Watcher() {
					public void process(WatchedEvent event) {
					if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
					connSignal.countDown();
					}
					}
					});

		} catch (Exception e) { 
			LOG.error("Problems Connecting with ZK");
		}
	}

	public void setupZk() {
		try {
			connSignal.await();
			if (zk.exists("/chunks", false) == null) {
				zk.create("/chunks", (new String("processing")).getBytes(), Ids.OPEN_ACL_UNSAFE, 
						CreateMode.PERSISTENT);

			}
			zk.create(fullPath, (new String("processing")).getBytes(), Ids.OPEN_ACL_UNSAFE, 
					CreateMode.PERSISTENT);
		} catch (Exception e) { 
			LOG.error("Problems creating /chunks ZNODE");
			e.printStackTrace();
		}
	}


	public void deleteChunks() {
		try {
			connSignal.await();

			LOG.info("fullPath = " + fullPath);

			if (zk.exists(fullPath, false) != null) {
				Transaction trans = zk.transaction();
				List<String> children = zk.getChildren(fullPath, false);

				int numChildren = 0;
				for (String child : children) {
					List<String> children2 = zk.getChildren(fullPath + "/" + child, false);
					for(String child2 : children2) {
						trans.delete(fullPath + "/" + child + "/" + child2, -1);
						numChildren++;
					}
					trans.delete(fullPath + "/" + child, -1);
				}
				trans.delete(fullPath, -1);
				trans.commit();

				LOG.info("Delete " + numChildren + " locks");
			}

		} catch (Exception e) { 
			LOG.error("Problems deleting /chunks ZNODE " + fullPath);
			e.printStackTrace();
		}
	}

	public void close() {
		try {
			zk.close();
		} catch (Exception e) {
			LOG.info("Failed to close connection to ZooKeeper server");
		}
	}
}
