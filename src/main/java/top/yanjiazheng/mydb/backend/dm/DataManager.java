package top.yanjiazheng.mydb.backend.dm;

import top.yanjiazheng.mydb.backend.dm.dataItem.DataItem;
import top.yanjiazheng.mydb.backend.dm.logger.Logger;
import top.yanjiazheng.mydb.backend.dm.page.PageOne;
import top.yanjiazheng.mydb.backend.dm.pageCache.PageCache;
import top.yanjiazheng.mydb.backend.tm.TransactionManager;

public interface DataManager {
    DataItem read(long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void close();

    // 静态方法，用于创建DataManager实例
    static DataManager create(String path, long mem, TransactionManager tm) {
        // 创建一个PageCache实例，path是文件路径，mem是内存大小
        PageCache pc = PageCache.create(path, mem);
        // 创建一个Logger实例，path是文件路径
        Logger lg = Logger.create(path);

        // 创建一个DataManagerImpl实例，pc是PageCache实例，lg是Logger实例，tm是TransactionManager实例
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        // 初始化PageOne
        dm.initPageOne();
        // 返回创建的DataManagerImpl实例
        return dm;
    }

    static DataManager open(String path, long mem, TransactionManager tm) {
        // 打开一个PageCache实例，path是文件路径，mem是内存大小
        PageCache pc = PageCache.open(path, mem);
        // 打开一个Logger实例，path是文件路径
        Logger lg = Logger.open(path);
        // 创建一个DataManagerImpl实例，pc是PageCache实例，lg是Logger实例，tm是TransactionManager实例
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        // 加载并检查PageOne，如果检查失败，则进行恢复操作
        if (!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        // 填充PageIndex，遍历从第二页开始的每一页，将每一页的页面编号和空闲空间大小添加到 PageIndex 中
        dm.fillPageIndex();
        // 设置PageOne为打开状态
        PageOne.setVcOpen(dm.pageOne);
        // 将PageOne立即写入到磁盘中，确保PageOne的数据被持久化
        dm.pc.flushPage(dm.pageOne);

        // 返回创建的DataManagerImpl实例
        return dm;
    }
}
