package com.aper.zookeeper.learning;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

public class ZkDistributeQueue extends AbstractQueue<String> implements BlockingQueue<String>, Serializable {
    private ZkClient zkClient;

    /**
     *  定义在zk上的znode，作为分布式队列的根目录。
     */
    private String queueRootNode;
    private static final String default_queueRootNode = "/distributeQueue";

    /**队列写锁节点*/
    private String queueWriteLockNode;
    /**队列读锁节点*/
    private String queueReadLockNode;
    /**
     * 子目录存放队列下的元素，用顺序节点作为子节点。
     */
    private String queueElementNode;

    /**
     * ZK服务的连接字符串，hostname:port形式的字符串
     */
    private String zkConnUrl;

    private static final String default_zkConnUrl = "localhost:2181";

    /**
     * 队列容量大小，默认Integer.MAX_VALUE，无界队列。
     * 注意Integer.MAX_VALUE其实也是有界的，存在默认最大值
     **/
    private static final int default_capacity = Integer.MAX_VALUE;
    private int capacity;

    /**
     * 控制进程访问的分布式锁
     */
    final Lock distributeWriteLock;
    final Lock distributeReadLock;

    public ZkDistributeQueue() {
        this(default_zkConnUrl, default_queueRootNode, default_capacity);
    }

    public ZkDistributeQueue(String zkServerUrl, String rootNodeName, int initCapacity) {
        if (zkServerUrl == null) throw new IllegalArgumentException("zkServerUrl");
        if (rootNodeName == null) throw new IllegalArgumentException("rootNodeName");
        if (initCapacity <= 0) throw new IllegalArgumentException("initCapacity");
        this.zkConnUrl = zkServerUrl;
        this.queueRootNode = rootNodeName;
        this.capacity = initCapacity;
        init();
        distributeWriteLock = new ZkImprovedDistributedLock(queueWriteLockNode);
        distributeReadLock = new ZkImprovedDistributedLock(queueReadLockNode);
    }

    /**
     * 初始化队列信息
     */
    private void init() {
        queueWriteLockNode = queueRootNode+"/writeLock";
        queueReadLockNode = queueRootNode+"/readLock";
        queueElementNode = queueRootNode+"/element";
        zkClient = new ZkClient(zkConnUrl);
        zkClient.setZkSerializer(new MyZkSerializer());
        if (!this.zkClient.exists(queueElementNode)) {
            try {
                this.zkClient.createPersistent(queueElementNode, true);
            } catch (ZkNodeExistsException e) {
                e.printStackTrace();
            }
        }
    }




    @Override
    public Iterator<String> iterator() {
        return null;
    }

    public int size() {
        int size = zkClient.countChildren(queueElementNode);
        return size;
    }


    private static void checkElement(String v) {
        if (v == null) throw new NullPointerException();
        if("".equals(v.trim())) {
            throw new IllegalArgumentException("不能使用空格");
        }
        if(v.startsWith(" ") || v.endsWith(" ")) {
            throw new IllegalArgumentException("前后不能包含空格");
        }
    }

    /**
     * 队列容量满了，不能再插入元素，阻塞等待队列移除元素。
     */
    private void waitForRemove() {
        CountDownLatch cdl = new CountDownLatch(1);
        // 注册watcher
        IZkChildListener listener = new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                if(currentChilds.size() < capacity) {	// 有任务移除，激活等待的添加操作
                    cdl.countDown();
                    System.out.println(Thread.currentThread().getName() + "-----监听到队列有元素移除，唤醒阻塞生产者线程");
                }
            }
        };
        zkClient.subscribeChildChanges(queueElementNode, listener);

        try {
            // 确保队列是满的
            if(size() >= capacity) {
                System.out.println(Thread.currentThread().getName() + "-----队列已满，阻塞等待队列元素释放");
                cdl.await();	// 阻塞等待元素被移除
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        zkClient.unsubscribeChildChanges(queueElementNode, listener);
    }

    /**
     * 往zk中添加元素
     * @param e
     */
    private void enqueue(String e) {
        zkClient.createPersistentSequential(queueElementNode+"/", e);
    }


    // 阻塞操作
    @Override
    public void put(String e) throws InterruptedException {
        checkElement(e);

        //尝试去获取分布式锁
        distributeWriteLock.lock();
        try {
            if(size() < capacity) {	// 容量足够
                enqueue(e);
                System.out.println(Thread.currentThread().getName() + "-----往队列放入了元素");
            }else { // 容量不够，阻塞，监听元素出队
                waitForRemove();
                put(e);
            }
        } finally {

            //释放锁
            distributeWriteLock.unlock();
        }
    }


    @Override
    public boolean offer(String s, long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public String take() throws InterruptedException {

        //老套路，先获取锁
        distributeReadLock.lock();
        try {
            List<String> children = zkClient.getChildren(queueElementNode);
            if(children != null && !children.isEmpty()) {

                //先对children进行一个排序，然后取出第一个，也就是最小编号的节点
                children = children.stream().sorted().collect(Collectors.toList());
                String takeChild = children.get(0);
                String childNode = queueElementNode+"/"+takeChild;
                String elementData = zkClient.readData(childNode);

                //进行出队操作
                dequeue(childNode);
                System.out.println(Thread.currentThread().getName() + "-----移除队列元素");
                return elementData;
            }else {

                //如果children本来就是空的，那就是没有元素需要消费，那就继续等待
                waitForAdd();		// 阻塞等待队列有元素加入
                return take();
            }
        } finally {
            distributeReadLock.unlock();
        }
    }

//出队操作

    private boolean dequeue(String e) {
        boolean result = zkClient.delete(e);
        return result;
    }


    @Override
    public String poll(long timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }

    @Override
    public int remainingCapacity() {
        return 0;
    }

    @Override
    public int drainTo(Collection<? super String> c) {
        return 0;
    }

    @Override
    public int drainTo(Collection<? super String> c, int maxElements) {
        return 0;
    }

    @Override
    public boolean offer(String s) {
        return false;
    }

    @Override
    public String poll() {
        return null;
    }

    @Override
    public String peek() {
        return null;
    }
}
