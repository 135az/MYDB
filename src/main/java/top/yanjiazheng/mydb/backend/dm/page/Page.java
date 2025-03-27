package top.yanjiazheng.mydb.backend.dm.page;

// 页面（Page）是存储在内存中的数据单元，其结构包括：
// 
// - pageNumber：页面的页号，从**1**开始计数。
// - data：实际包含的字节数据。
// - dirty：标志着页面是否是脏页面，在缓存驱逐时，脏页面需要被写回磁盘。
// - lock：用于页面的锁。
// - PageCache：保存了一个 PageCache 的引用，方便在拿到 Page 的引用时可以快速对页面的缓存进行释放操作。
public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
