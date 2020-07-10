package com.aper.zookeeper.learning;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class ZkDistributeLock implements Lock {

    //我们需要一个锁的目录
    private String lockPath;

    //我们需要一个客户端
    private ZkClient client;


    //刚刚我们的客户端和锁的目录，这两个参数怎么传进来？
    //那就需要我们的构造函数来进行传值

    public ZkDistributeLock(String lockPath) {
        if (lockPath == null || lockPath.trim().equals("")) {
            throw new IllegalArgumentException("patch不能为空字符串");
        }
        this.lockPath = lockPath;

        client = new ZkClient("localhost:2181");
        client.setZkSerializer(new MyZkSerializer());
    }


    // trylock方法我们是会尝试创建一个临时节点
    @Override
    public boolean tryLock() { // 不会阻塞
        // 创建节点
        try {
            client.createEphemeral(lockPath);
        } catch (ZkNodeExistsException e) {
            return false;
        }
        return true;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public void unlock() {
        client.delete(lockPath);
    }

    @Override
    public Condition newCondition() {
        return null;
    }


    @Override
    public void lock() {

        // 如果获取不到锁，阻塞等待
        if (!tryLock()) {

            // 没获得锁，阻塞自己
            waitForLock();

            // 从等待中唤醒，再次尝试获得锁
            lock();
        }

    }

    @Override
    public void lockInterruptibly() throws InterruptedException {

    }

    private void waitForLock() {
        final CountDownLatch cdl = new CountDownLatch(1);

        IZkDataListener listener = new IZkDataListener() {

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                System.out.println("----收到节点被删除了-------------");
                //唤醒阻塞线程
                cdl.countDown();
            }

            @Override
            public void handleDataChange(String dataPath, Object data)
                    throws Exception {
            }
        };

        client.subscribeDataChanges(lockPath, listener);

        // 阻塞自己
        if (this.client.exists(lockPath)) {
            try {
                cdl.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        // 取消注册
        client.unsubscribeDataChanges(lockPath, listener);
    }

}