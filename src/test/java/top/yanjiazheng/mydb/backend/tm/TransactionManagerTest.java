package top.yanjiazheng.mydb.backend.tm;

import java.io.File;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.Test;

public class TransactionManagerTest {

    static Random random = new SecureRandom();

    private int transCnt = 0;
    private int noWorkers = 50;
    private int noWorks = 3000;
    private Lock lock = new ReentrantLock();
    private TransactionManager tmger;
    private Map<Long, Byte> transMap;
    private CountDownLatch cdl;

    // 测试多线程环境下TransactionManager的性能和正确性
    @Test
    public void testMultiThread() {
        // 初始化TransactionManager，参数为交易管理器的存储路径
        tmger = TransactionManager.create("/tmp/tranmger_test");
        // 初始化并发安全的Map，用于存储线程相关的交易信息
        transMap = new ConcurrentHashMap<>();
        // 初始化CountDownLatch，用于主线程等待所有工作线程完成任务
        cdl = new CountDownLatch(noWorkers);
        // 创建并启动指定数量的工作线程
        for(int i = 0; i < noWorkers; i ++) {
            // 使用Lambda表达式定义线程任务
            Runnable r = () -> worker();
            // 创建线程并启动
            new Thread(r).run();
        }
        // 主线程等待所有工作线程完成任务
        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 测试结束后，删除交易管理器的存储文件
        assert new File("/tmp/tranmger_test.xid").delete();
    }

    /**
     * 执行工作者线程的任务
     * 该方法模拟事务的开始、提交、回滚以及检查事务状态的操作
     */
    private void worker() {
        // 标记当前是否在事务中
        boolean inTrans = false;
        // 当前事务的ID
        long transXID = 0;
        // 循环执行预定数量的工作任务
        for(int i = 0; i < noWorks; i ++) {
            // 随机生成一个操作码，决定接下来的操作类型
            int op = Math.abs(random.nextInt(6));
            // 对事务操作加锁，确保线程安全
            lock.lock();
            if(op == 0) {
                // 如果当前不在事务中，则开始一个新的事务
                if(!inTrans) {
                    long xid = tmger.begin();
                    transMap.put(xid, (byte)0);
                    transCnt ++;
                    transXID = xid;
                    inTrans = true;
                } else {
                    // 如果当前在事务中，则根据随机数决定是提交还是回滚事务
                    int status = (random.nextInt(Integer.MAX_VALUE) % 2) + 1;
                    switch(status) {
                        case 1:
                            tmger.commit(transXID);
                            break;
                        case 2:
                            tmger.abort(transXID);
                            break;
                    }
                    transMap.put(transXID, (byte)status);
                    inTrans = false;
                }
                // 解锁事务操作
            } else {
                // 如果存在已开始的事务，则随机选择一个事务检查其状态
                if(transCnt > 0) {
                    long xid = (long)((random.nextInt(Integer.MAX_VALUE) % transCnt) + 1);
                    byte status = transMap.get(xid);
                    boolean ok = false;
                    switch (status) {
                        case 0:
                            ok = tmger.isActive(xid);
                            break;
                        case 1:
                            ok = tmger.isCommitted(xid);
                            break;
                        case 2:
                            ok = tmger.isAborted(xid);
                            break;
                    }
                    // 断言事务状态检查结果应为真
                    assert ok;
                }
                // 解锁事务状态检查
            }
            lock.unlock();
        }
        // 工作完成后，计数器减一，用于同步控制
        cdl.countDown();
    }
}
