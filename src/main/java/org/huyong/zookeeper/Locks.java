package org.huyong.zookeeper;


import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class Locks implements Watcher {

    private static final String URL = "192.168.11.22:2181";

    static ZooKeeper zk = null;
    static Integer mutex = null;
    String name = null;
    String path = null;

    @Override
    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            mutex.notify();
        }
    }

    Locks(String address) {
        try {
            zk = new ZooKeeper(address, 2000, this);
            zk.create("/lock", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            mutex = new Integer(-1);
            name = new String(InetAddress.getLocalHost().getCanonicalHostName().toString());
        } catch (IOException e) {
            zk = null;
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private int minSeq(List<String> list) {
        int min = Integer.parseInt(list.get(0).substring(14));
        for (int i = 1; i < list.size(); i++) {
            if (min < Integer.parseInt(list.get(i).substring(14)))
                min = Integer.parseInt(list.get(i).substring(14));
        }
        return min;
    }

    boolean getLock() throws KeeperException, InterruptedException {
        //create方法返回新建的节点的完整路径
        path = zk.create("/lock/" + name + "-", new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        int min;
        while (true) {
            synchronized (mutex) {
                List<String> list = zk.getChildren("/lock", false);
                min = minSeq(list);
                //如果刚建的节点是根节点的所有子节点中序号最小的，则获得了锁，可以返回true
                if (min == Integer.parseInt(path.substring(14))) {
                    return true;
                } else {
                    mutex.wait(); //等待事件（新建节点或删除节点）发生
                    while (true) {
                        Stat s = zk.exists("/lock/" + name + "-" + min, true); //查看序号最小的子节点还在不在
                        if (s != null) //如果还在，则继续等待事件发生
                            mutex.wait();
                        else //如果不在，则跳外层循环中，查看新的最小序号的子节点是谁
                            break;
                    }
                }
            }
        }
    }

    boolean releaseLock() throws KeeperException, InterruptedException {
        if (path != null) {
            zk.delete(path, -1);
            path = null;
        }
        return true;
    }

    public static void main(String[] args) throws KeeperException, InterruptedException {
        Locks lock1 = new Locks(URL);
        if (lock1.getLock()) {
            System.out.println("T1 Get lock at " + System.currentTimeMillis());
            for (int i = 0; i < 1000; ++i)
                Thread.sleep(5000);
            lock1.releaseLock();
        }
        Locks lock2 = new Locks(URL);
        if (lock2.getLock()) {
            System.out.println("T2 Get lock at " + System.currentTimeMillis());
            lock2.releaseLock();
        }
    }

}
