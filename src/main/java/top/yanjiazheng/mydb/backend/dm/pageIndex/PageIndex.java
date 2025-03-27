package top.yanjiazheng.mydb.backend.dm.pageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.yanjiazheng.mydb.backend.dm.pageCache.PageCache;

public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists;

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }
    
    
    /**
    * 根据给定的页面编号和空闲空间大小添加一个 PageInfo 对象。
    * @param pgno      页面编号
    * @param freeSpace 页面的空闲空间大小
    */
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            // 计算空闲空间大小对应的区间编号
            int number = freeSpace / THRESHOLD;
            // 在对应的区间列表中添加一个新的 PageInfo 对象
            lists[number].add(new PageInfo(pgno, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    /**
     * 根据给定的空间大小选择一个 PageInfo 对象。
     *
     * @param spaceSize 需要的空间大小
    * @return 一个 PageInfo 对象，其空闲空间大于或等于给定的空间大小。如果没有找到合适的 PageInfo，返回 null。
    */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            // 计算需要的空间大小对应的区间编号
            int number = spaceSize / THRESHOLD;
            // 如果计算出的区间编号小于总的区间数，编号加一
            if(number < INTERVALS_NO) number ++;
            // 从计算出的区间编号开始，向上寻找合适的 PageInfo
            while(number <= INTERVALS_NO) {
                // 如果当前区间没有 PageInfo，继续查找下一个区间
                if(lists[number].isEmpty()) {
                    number ++;
                    continue;
                }
                // 如果当前区间有 PageInfo，返回第一个 PageInfo，并从列表中移除
                return lists[number].remove(0);
            }
            // 如果没有找到合适的 PageInfo，返回 null
            return null;
        } finally {
            lock.unlock();
        }
    }

}
