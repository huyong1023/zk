package org.huyong.zookeeper;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class SyncPrimitive implements Watcher {

    private static final String URL = "192.168.11.22:2181";


    static ZooKeeper zk = null;

    static Integer mutex;

    String root;
    //同步原语


    SyncPrimitive(String address) {
        if (zk == null) {
            try {
                System.out.println("Starting ZK:");
                //建立Zookeeper连接，并且指定watcher
                zk = new ZooKeeper(address, 3000, this);
                //初始化锁对象
                mutex = new Integer(-1);
                System.out.println("Finished starting ZK:" + zk);
            } catch (IOException e) {
                System.out.println(e.toString());
                zk = null;
            }
        }
    }


    @Override


    synchronized public void process(WatchedEvent event) {
        synchronized (mutex) {
            //有事件发生时，调用notify，使其他wait()点得以继续
            mutex.notify();
        }

    }


    static public class Barrier extends SyncPrimitive {

        int size;

        String name;


        Barrier(String address, String root, int size) {
            super(address);
            this.root = root;
            this.size = size;
            if (zk != null) {
                try {
                    //一个barrier建立一个根目录
                    Stat s = zk.exists(root, false); //不注册watcher
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out
                            .println("keeper exception when instantiating queue:"
                                    + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception.");
                }
            }
            try {
                //获取自己的主机名
                name = new String(InetAddress.getLocalHost()
                        .getCanonicalHostName().toString());
            } catch (UnknownHostException e) {
                System.out.println(e.toString());
            }
        }


        boolean enter() throws KeeperException, InterruptedException {
            //在根目录下创建一个子节点.create和delete都会触发children wathes,这样getChildren就会收到通知，process()就会被调用
            zk.create(root + "/" + name, new byte[0], Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL_SEQUENTIAL);
            //一直等，直到根目录下的子节点数目达到size时，函数退出
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                    if (list.size() < size) {
                        mutex.wait(); //释放mutex上的锁
                    } else {
                        return true;
                    }
                }
            }
        }


        boolean leave() throws KeeperException, InterruptedException {
            //删除自己创建的节点
            zk.delete(root + "/" + name, 0);
            //一直等，直到根目录下有子节点时，函数退出
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true);
                    if (list.size() > 0) {
                        mutex.wait();
                    } else {
                        return true;
                    }
                }
            }
        }


    }


    static public class Queue extends SyncPrimitive {


        Queue(String address, String name) {
            super(address);
            this.root = name;
            if (zk != null) {
                try {
                    //一个queue建立一个根目录
                    Stat s = zk.exists(root, false);
                    if (s == null) {
                        zk.create(root, new byte[0], Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                    }
                } catch (KeeperException e) {
                    System.out
                            .println("keeper exception when instantiating queue:"
                                    + e.toString());
                } catch (InterruptedException e) {
                    System.out.println("Interrupted exception.");
                }
            }
        }


        //参数i是要创建节点的data


        boolean produce(int i) throws KeeperException, InterruptedException {
            ByteBuffer b = ByteBuffer.allocate(4);
            byte[] value;
            b.putInt(i);
            value = b.array();

            //根目录下创建一个子节点，因为是SEQUENTIAL的，所以先创建的节点具有较小的序号
            zk.create(root + "/element", value, Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT_SEQUENTIAL);
            return true;
        }


        int consume() throws KeeperException, InterruptedException {
            int retvalue = -1;
            Stat stat = null;
            while (true) {
                synchronized (mutex) {
                    List<String> list = zk.getChildren(root, true); //并不能保证list[0]就是序号最小的
                    //如果根目录下没有子节点就一直等
                    if (list.size() == 0) {
                        System.out.println("Going to wait");
                        mutex.wait();
                    }
                    //找到序号最小的节点将其删除
                    else {
                        Integer min = new Integer(list.get(0).substring(7));
                        for (String s : list) {
                            Integer tmp = new Integer(s.substring(7));
                            if (tmp < min)
                                min = tmp;
                        }
                        System.out.println("Temporary value:" + root
                                + "/element" + min);
                        byte[] b = zk.getData(root + "/element" + min, false,
                                stat);
                        zk.delete(root + "/element" + min, 0);
                        ByteBuffer buffer = ByteBuffer.wrap(b);
                        retvalue = buffer.getInt();
                        return retvalue;
                    }
                }
            }
        }


    }


    public static void main(String[] args) {

        String[] arg = {"qTest", URL, "10", "consume"};
        if (arg[0].equals("qTest"))
            queueTest(arg);
        else
            barrierTest(arg);
    }


    private static void barrierTest(String[] args) {
        Barrier b = new Barrier(args[1], "/b1", new Integer(args[2]));
        try {
            boolean flag = b.enter();
            System.out.println("Enter barrier:" + args[2]);
            if (!flag)
                System.out.println("Error when entering the barrier");
        } catch (KeeperException e) {
            System.out.println(e);
        } catch (InterruptedException e) {
            System.out.println(e);
        }

        Random rand = new Random();
        int r = rand.nextInt(100);
        for (int i = 0; i < r; i++) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                System.out.println(e);
            }
        }
        try {
            b.leave();
        } catch (KeeperException e) {

        } catch (InterruptedException e) {
        }
        System.out.println("Left barrier");
    }


    private static void queueTest(String[] args) {
        Queue q = new Queue(args[1], "/app1");
        System.out.println("Input:" + args[1]);
        int i;
        Integer max = new Integer(args[2]);

        if (args[3].equals("p")) {
            System.out.println("Producer");
            for (i = 0; i < max; i++)
                try {
                    q.produce(10 + 1);
                } catch (KeeperException e) {

                } catch (InterruptedException e) {
                }
        } else {
            System.out.println("Consumer");
            for (i = 0; i < max; i++)
                try {
                    int r = q.consume();
                    System.out.println("Item:" + r);
                } catch (KeeperException e) {
                    System.out.print(e);
                    i--;
                } catch (InterruptedException e) {
                    System.out.print(e);
                }
        }
    }

}