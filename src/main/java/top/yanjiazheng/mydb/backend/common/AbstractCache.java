package top.yanjiazheng.mydb.backend.common;

import top.yanjiazheng.mydb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 */
public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 元素的引用个数
    private HashMap<Long, Boolean> getting;             // 正在获取某资源的线程

    private int maxResource;                            // 缓存的最大缓存资源数
    private int count = 0;                              // 缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 根据键获取缓存对象如果对象不在缓存中，则尝试从资源中获取并放入缓存
     *
     * @param key 要获取的对象的键
     * @return 对应键的缓存对象
     * @throws Exception 如果获取对象时发生错误
     */
    protected T get(long key) throws Exception {
        while (true) {
            lock.lock();
            if (getting.containsKey(key)) {
                // 如果其他线程正在获取这个资源，那么当前线程将等待一毫秒然后继续循环
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if (cache.containsKey(key)) {
                // 如果资源已经在缓存中，直接返回资源，并增加引用计数
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            // 如果资源不在缓存中，尝试获取资源。如果缓存已满，抛出异常
            if (maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            count++;
            getting.put(key, true);
            lock.unlock();
            break;
        }

        // 尝试获取资源
        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        // 将获取到的资源添加到缓存中，并设置引用计数为1
        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();

        return obj;
    }

    /**
     * 强行释放一个缓存
     */
    protected void release(long key) {
        lock.lock(); // 获取锁
        try {
            int ref = references.get(key) - 1; // 获取资源的引用计数并减一
            if (ref == 0) { // 如果引用计数为0
                T obj = cache.get(key); // 从缓存中获取资源
                releaseForCache(obj); // 处理资源的释放
                references.remove(key); // 从引用计数的映射中移除资源
                cache.remove(key); // 从缓存中移除资源
                count--; // 将缓存中的资源计数减一
            } else { // 如果引用计数不为0
                references.put(key, ref); // 更新资源的引用计数。
            }
        } finally {
            lock.unlock(); // 释放锁
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
