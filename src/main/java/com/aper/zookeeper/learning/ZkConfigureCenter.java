package com.aper.zookeeper.learning;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;

import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

interface ConfigureWriter {
    /**
     * 创建一个新的配置文件
     *
     * @param fileName 文件名称
     * @param items    配置项
     * @return 新文件的在zk上的路径
     */
    String createCnfFile(String fileName, Properties items);

    /**
     * 删除一个配置文件
     *
     * @param fileName
     */
    void deleteCnfFile(String fileName);

    /**
     * 修改一个配置文件
     *
     * @param fileName
     * @param items
     */
    void modifyCnfItem(String fileName, Properties items);

    /**
     * 加载配置文件
     *
     * @param fileName
     * @return
     */
    Properties loadCnfFile(String fileName);
}

/**
 * 配置文件读取器
 * ConfigureReader
 */
interface ConfigureReader {
    /**
     * 读取配置文件
     * @param fileName 配置文件名称
     * @return 如果存在文件配置，则返回Properties对象，不存在返回null
     */
    Properties loadCnfFile(String fileName);
    /**
     * 监听配置文件变化，此操作只需要调用一次。
     * @param fileName
     * @param changeHandler
     */
    void watchCnfFile(String fileName, ChangeHandler changeHandler);


}
/**
 * 配置文件变化处理器
 * ChangeHandler
 */
interface ChangeHandler {
    /**
     * 配置文件发生变化后给一个完整的属性对象
     * @param newProp
     */
    void itemChange(Properties newProp);
}

class ConfigureTest {

    public static void main(String[] args) {
        // 模拟运维人员创建配置文件，引用ConfigureWriter接口操作
        ConfigureWriter writer = new ZkConfigureCenter();
        String fileName = "trade-application.properties";
        writer.deleteCnfFile(fileName);	// 测试，确保配置中心没有这个问题

        Properties items = new Properties();
        items.put("abc.gc.a", "123");
        items.put("abc.gc.b", "3456");
        // 创建配置文件，内容为 properties items的内容。
        String znodePath = writer.createCnfFile(fileName, items);
        System.out.println("new file: "+znodePath);


        new Thread(()->{
            readCnf();
        }).start();

        try {
            Thread.sleep(3000L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 3秒后修改文件内容，有新增、有删除、有修改
        items.put("abc.gc.a", "haha");	// 修改
        items.put("abc.gc.c", "xx");	// 新增
        items.remove("abc.gc.b"); // 删除
        writer.modifyCnfItem(fileName, items);

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 模拟应用程序加载配置文件，监听配置文件的变化
     */
    public static void readCnf() {
        // 应用引用ConfigureReader接口进行操作
        System.out.println("读取并监听配置文件");
        ConfigureReader reader = new ZkConfigureCenter();
        String fileName = "trade-application.properties";
        Properties p = reader.loadCnfFile(fileName);		// 读取配置文件
        System.out.println(p);

        // 监听配置文件
        reader.watchCnfFile(fileName, new ChangeHandler() {
            @Override
            public void itemChange(Properties newProp) {
                System.out.println("发现数据发生变化："+ newProp);
            }
        });

        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}



public class ZkConfigureCenter implements ConfigureWriter, ConfigureReader {
    private String confRootPath;
    private String confFilePath;
    private String fileLockPath;
    private static final String default_confRootPath = "/distributeConfigure";
    private ZkClient client;
    public ZkConfigureCenter() {
        this(default_confRootPath);
    }

    public ZkConfigureCenter(String path) {
        if(path == null || path.trim().equals("")) {
            throw new IllegalArgumentException("patch不能为空字符串");
        }
        confRootPath = path;
        confFilePath = confRootPath+"/cnfFile";
        fileLockPath = confRootPath+"/writeLock";
        client = new ZkClient("localhost:2181");
        client.setZkSerializer(new MyZkSerializer());
        if (!this.client.exists(confFilePath)) {
            try {
                this.client.createPersistent(confFilePath, true);
            } catch (ZkNodeExistsException e) {
                e.printStackTrace();
            }
        }
    }

    //简单的参数检查
    private void checkElement(String v) {
        if (v == null) throw new NullPointerException();
        if("".equals(v.trim())) {
            throw new IllegalArgumentException("不能使用空格");
        }
        if(v.startsWith(" ") || v.endsWith(" ")) {
            throw new IllegalArgumentException("前后不能包含空格");
        }
    }


    @Override
    public String createCnfFile(String fileName, Properties items) {
        checkElement(fileName);
        // 创建配置文件Node
        String cfgNode = confFilePath+"/"+fileName;

        //如果配置文件已经存在，总不能把别人的给覆写掉吧
        if(client.exists(cfgNode)) {
            throw new IllegalArgumentException("["+fileName+"]文件已存在！");
        }

        //没问题了，创建持久节点
        client.createPersistent(cfgNode, true);
        // 创建配置文件中的配置项
        if(items == null) {return cfgNode;}

        //这里我们创建了带上这个配置文件名字的一把分布式锁，不同的文件名就意味着不同的锁
        // ZkDistributeImproveLock的实现(参考"从零开始的高并发（二）--- Zookeeper实现分布式锁")
        Lock distributeWriteLock = new ZkImprovedDistributedLock(fileLockPath+"/"+fileName);
        distributeWriteLock.lock();
        try {
            //以下就是对properties进行遍历然后把属性值一个个写进去而已
            //如果真的没看懂这个，建议使用IDEA进行debug一下，
            items.keySet().iterator();
            Set<Map.Entry<Object, Object>> entrySet = items.entrySet();
            for (Map.Entry<Object, Object> entry : entrySet) {
                System.out.println(entry.getKey() + "=" + entry.getValue());
                String cfgItemNode = cfgNode +"/"+ entry.getKey().toString();
                client.createPersistent(cfgItemNode, entry.getValue());
            }
        } finally {
            distributeWriteLock.unlock();
        }
        return cfgNode;
    }

    @Override
    public void deleteCnfFile(String fileName) {
        checkElement(fileName);
        String cfgNode = confFilePath+"/"+fileName;
        Lock distributeWriteLock = new ZkImprovedDistributedLock(fileLockPath+"/"+fileName);

        //获取锁
        distributeWriteLock.lock();
        try {
            client.deleteRecursive(cfgNode);
        } finally {
            //释放锁
            distributeWriteLock.unlock();
        }
    }

    @Override
    public void modifyCnfItem(String fileName, Properties items) {
        checkElement(fileName);
        // 获取子节点信息
        String cfgNode = confFilePath+"/"+fileName;
        // 简单粗暴的实现
        if(items == null) {throw new NullPointerException("要修改的配置项不能为空");}
        items.keySet().iterator();
        Set<Map.Entry<Object, Object>> entrySet = items.entrySet();
        Lock distributeWriteLock = new ZkImprovedDistributedLock(fileLockPath+"/"+fileName);
        distributeWriteLock.lock();
        try {
            // 获取zk中已存在的配置信息
            List<String> itemNodes = client.getChildren(cfgNode);
            Set<String> existentItemSet = new HashSet<>(itemNodes);

            for (Map.Entry<Object, Object> entry : entrySet) {
                System.out.println(entry.getKey() + "=" + entry.getValue());
                String itemName = entry.getKey().toString();
                String itemData = entry.getValue().toString();

                String cfgItemNode = cfgNode + "/" + itemName;
                if(existentItemSet.contains(itemName)) {// zk中存在的配置项
                    String itemNodeData = client.readData(cfgItemNode);
                    if(! itemNodeData.equals(itemData) ) { // 数据不一致才需要修改
                        client.writeData(cfgItemNode, itemData);
                    }
                    existentItemSet.remove(itemName);	// 剩下的就是需要删除的配置项
                } else { // zk中不存在的配置项，新的配置项
                    client.createPersistent(cfgItemNode, itemData);
                }
            }

            // existentItemSet中剩下的就是需要删除的
            if(!existentItemSet.isEmpty()) {
                for(String itemName : existentItemSet) {
                    String cfgItemNode = cfgNode + "/" + itemName;
                    client.delete(cfgItemNode);
                }
            }
        } finally {
            distributeWriteLock.unlock();
        }
    }


    @Override
    public Properties loadCnfFile(String fileName) {
        if(! fileName.startsWith("/")) {
            fileName = confFilePath+"/"+fileName;
        }
        return loadNodeCnfFile(fileName);
    }

    private Properties loadNodeCnfFile(String cfgNode) {
        checkElement(cfgNode);
        if(! client.exists(cfgNode)) {
            throw new ZkNoNodeException(cfgNode);
        }
        // 获取子节点信息
        List<String> itemNodes = client.getChildren(cfgNode);

        // 读取配置信息，并装载到Properties中
        if(itemNodes == null || itemNodes.isEmpty()) {
            return new Properties();
        }
        Properties file = new Properties();
        itemNodes.forEach((e)->{
            String itemNameNode = cfgNode + "/" + e;
            String data = client.readData(itemNameNode, true);
            file.put(e, data);
        });
        return file;
    }

    @Override
    public void watchCnfFile(String fileName, ChangeHandler changeHandler) {
        if(! fileName.startsWith("/")) {
            fileName = confFilePath+"/"+fileName;
        }
        final String fileNodePath = fileName;
        // 读取文件
        Properties p = loadNodeCnfFile(fileNodePath);
        // 合并5秒配置项变化，5秒内变化只触发一次处理事件
        int waitTime = 5;
        final ScheduledThreadPoolExecutor scheduled = new ScheduledThreadPoolExecutor(1);
        scheduled.setRemoveOnCancelPolicy(true);
        final List<ScheduledFuture<?>> futureList = new ArrayList<ScheduledFuture<?>>();
        Set<Map.Entry<Object, Object>> entrySet = p.entrySet();
        for (Map.Entry<Object, Object> entry : entrySet) {
            System.out.println("监控："+fileNodePath+"/"+entry.getKey().toString());
            client.subscribeDataChanges(fileNodePath+"/"+entry.getKey().toString(), new IZkDataListener() {
                @Override
                public void handleDataDeleted(String dataPath) throws Exception {
                    System.out.println("触发删除："+dataPath);
                    triggerHandler(futureList, scheduled, waitTime, fileNodePath, changeHandler);
                }

                @Override
                public void handleDataChange(String dataPath, Object data) throws Exception {
                    System.out.println("触发修改："+dataPath);
                    triggerHandler(futureList, scheduled, waitTime, fileNodePath, changeHandler);
                }
            });
        }
        client.subscribeChildChanges(fileNodePath, new IZkChildListener() {
            @Override
            public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
                System.out.println("触发子节点："+parentPath);
                triggerHandler(futureList, scheduled, waitTime, fileNodePath, changeHandler);
            }
        });
    }

    /**
     * 合并修改变化事件，5秒钟内发生变化的合并到一个事件进行
     * @param futureList 装有定时触发任务的列表
     * @param scheduled 定时任务执行器
     * @param waitTime 延迟时间，单位秒
     * @param fileName zk配置文件的节点
     * @param changeHandler 事件处理器
     */
    private void triggerHandler(List<ScheduledFuture<?>> futureList, ScheduledThreadPoolExecutor scheduled, int waitTime, String fileName, ChangeHandler changeHandler) {
        if(futureList != null && !futureList.isEmpty()) {
            for(int i = 0 ; i < futureList.size(); i++) {
                ScheduledFuture<?> future = futureList.get(i);
                if(future != null && !future.isCancelled() && !future.isDone()) {
                    future.cancel(true);
                    futureList.remove(future);
                    i--;
                }
            }
        }
        ScheduledFuture<?> future = scheduled.schedule(()->{
            Properties p = loadCnfFile(fileName);
            changeHandler.itemChange(p);
        }, waitTime, TimeUnit.SECONDS);
        futureList.add(future);
    }




}