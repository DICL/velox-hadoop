package org.dicl.velox.mapreduce;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Transaction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.List;

public class LeanSession {
    private static final Log LOG = LogFactory.getLog(LeanSession.class);
    private ZooKeeper zk;

    public LeanSession(String addr, int timeout) {
        CountDownLatch connSignal = new CountDownLatch(0);
        try {
        zk = new ZooKeeper(addr, timeout, new Watcher() {
            public void process(WatchedEvent event) {
                if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connSignal.countDown();
                }
            }
        });

        } catch (Exception e) { 
        }

        try {
            connSignal.await();
            deleteChunks();
            zk.create("/chunks", (new String("processing")).getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (Exception e) { 
            LOG.error("Problems creating /chunks ZNODE");
            e.printStackTrace();
        }
    }

    public void deleteChunks() throws Exception {
       if (zk.exists("/chunks", false) != null) {
           Transaction trans = zk.transaction();
           List<String> children = zk.getChildren("/chunks", false);
           int numChildren = 0;
           for (String child : children) {
               trans.delete("/chunks/"+ child, -1);
               numChildren++;
           }
           trans.delete("/chunks", -1);
           trans.commit();

           LOG.info("Delete " + numChildren + " locks");
       }
    }

    public void close() {
        try {
            zk.close();
        } catch (Exception e) { }
    }
}
