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

    private void worker() {
        boolean inTrans = false;
        long transXID = 0;
        for(int i = 0; i < noWorks; i ++) {
            int op = Math.abs(random.nextInt(6));
            if(op == 0) {
                lock.lock();
                if(inTrans == false) {
                    long xid = tmger.begin();
                    transMap.put(xid, (byte)0);
                    transCnt ++;
                    transXID = xid;
                    inTrans = true;
                } else {
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
                lock.unlock();
            } else {
                lock.lock();
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
                    assert ok;
                }
                lock.unlock();
            }
        }
        cdl.countDown();
    }
}
