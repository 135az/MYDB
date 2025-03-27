package top.yanjiazheng.mydb.backend.dm;

import top.yanjiazheng.mydb.backend.common.AbstractCache;
import top.yanjiazheng.mydb.backend.dm.dataItem.DataItem;
import top.yanjiazheng.mydb.backend.dm.dataItem.DataItemImpl;
import top.yanjiazheng.mydb.backend.dm.logger.Logger;
import top.yanjiazheng.mydb.backend.dm.page.Page;
import top.yanjiazheng.mydb.backend.dm.page.PageOne;
import top.yanjiazheng.mydb.backend.dm.page.PageX;
import top.yanjiazheng.mydb.backend.dm.pageCache.PageCache;
import top.yanjiazheng.mydb.backend.dm.pageIndex.PageIndex;
import top.yanjiazheng.mydb.backend.dm.pageIndex.PageInfo;
import top.yanjiazheng.mydb.backend.tm.TransactionManager;
import top.yanjiazheng.mydb.backend.utils.Panic;
import top.yanjiazheng.mydb.backend.utils.Types;
import top.yanjiazheng.mydb.common.Error;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        // 从缓存页面中读取到DataItemImpl
        DataItemImpl di = (DataItemImpl) super.get(uid);
        // 校验di是否有效
        if (!di.isValid()) {
            // 无效释放缓存
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        // 将输入的数据包装成DataItem的原始格式
        byte[] raw = DataItem.wrapDataItemRaw(data);
        // 如果数据项的大小超过了页面的最大空闲空间，抛出异常
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        PageInfo pi = null;
        // 尝试5次找到一个可以容纳新数据项的页面
        for (int i = 0; i < 5; i++) {
            // 从页面索引中选择一个可以容纳新数据项的页面
            pi = pIndex.select(raw.length);
            // 如果找到了合适的页面，跳出循环
            if (pi != null) {
                break;
            } else {
                // 如果没有找到合适的页面，创建一个新的页面，并将其添加到页面索引中
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        // 如果还是没有找到合适的页面，抛出异常
        if (pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            // 获取页面信息对象中的页面，生成插入日志，将日志写入日志文件
            pg = pc.getPage(pi.pgno);
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);
            // 在页面中插入新的数据项，并获取其在页面中的偏移量
            short offset = PageX.insert(pg, raw);
            // 释放页面
            pg.release();
            // 返回新插入的数据项的唯一标识符
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将页面重新添加到页面索引中
            if (pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        // 从 uid 中提取出偏移量（offset），这是通过位操作实现的，偏移量是 uid 的低16位
        short offset = (short) (uid & ((1L << 16) - 1));
        // 将 uid 右移32位，以便接下来提取出页面编号（pgno）
        uid >>>= 32;
        // 从 uid 中提取出页面编号（pgno），页面编号是 uid 的高32位
        int pgno = (int) (uid & ((1L << 32) - 1));
        // 使用页面缓存（pc）的 getPage(int pgno) 方法根据页面编号获取一个 Page 对象
        Page pg = pc.getPage(pgno);
        // 使用 DataItem 接口的静态方法 parseDataItem(Page pg, short offset, DataManagerImpl dm)
        // 根据获取到的 Page 对象、偏移量和当前的 DataManagerImpl 对象（this）解析出一个 DataItem 对象，并返回这个对象
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /**
     * 填充 PageIndex。
     * 遍历从第二页开始的每一页，将每一页的页面编号和空闲空间大小添加到 PageIndex 中。
     */
    void fillPageIndex() {
        // 获取当前的页面数量
        int pageNumber = pc.getPageNumber();
        // 从第二页开始，对每一页进行处理
        for (int i = 2; i <= pageNumber; i++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            // 将页面编号和页面的空闲空间大小添加到 PageIndex 
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }

}
