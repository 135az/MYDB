package top.yanjiazheng.mydb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.yanjiazheng.mydb.common.Error;

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
        while(true) {
            lock.lock();
            if(getting.containsKey(key)) {
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
    
            if(cache.containsKey(key)) {
                // 资源在缓存中，直接返回
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }
    
            // 尝试获取该资源
            if(maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            count ++;
            getting.put(key, true);
            lock.unlock();
            break;
        }
    
        T obj = null;
        try {
            obj = getForCache(key);
        } catch(Exception e) {
            lock.lock();
            count --;
            getting.remove(key);
            lock.unlock();
            throw e;
        }
    
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
        lock.lock();
        try {
            int ref = references.get(key)-1;
            if(ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count --;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
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
