package top.yanjiazheng.mydb.backend.dm.page;

import java.util.Arrays;

import top.yanjiazheng.mydb.backend.dm.pageCache.PageCache;
import top.yanjiazheng.mydb.backend.utils.Parser;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 */
public class PageX {
    
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

    // 设置空闲空间偏移量
    private static void setFSO(byte[] raw, short ofData) {
        // 将空闲空间偏移量的值复制到字节数组的指定位置
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    // 获取pg的FSO
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 将原始字节数组插入到指定页面中，并返回插入的位置
     * 此方法首先将页面标记为已修改，然后找到页面中自由空间的起始点（FSO）
     * 接着将原始字节数组复制到页面数据的自由空间中，并更新自由空间的起始点位置
     * 
     * @param pg 要插入数据的目标页面，此页面会被修改
     * @param raw 要插入的原始字节数组
     * @return 返回插入操作开始的位置，即原始数据在页面中的起始偏移量
     */
    public static short insert(Page pg, byte[] raw) {
        // 将页面标记为已修改，表示页面数据已变更
        pg.setDirty(true);
        
        // 获取页面中空闲空间的起始偏移量
        short offset = getFSO(pg.getData());
        
        // 将raw的数据复制到pg的数据中的offset位置
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        
        // 更新pg的空闲空间偏移量，为下一次插入操作做准备
        setFSO(pg.getData(), (short)(offset + raw.length));
        
        // 返回插入操作的起始位置
        return offset;
    }

    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page pg) {
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }

    // 将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        // 将pg的dirty标志设置为true，表示pg的数据已经被修改
        pg.setDirty(true);
        // 将raw的数据复制到pg的数据中的offset位置
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        // 获取pg的当前空闲空间偏移量
        short rawFSO = getFSO(pg.getData());
        // 如果当前的空闲空间偏移量小于offset + raw.length
        if(rawFSO < offset + raw.length) {
            // 将pg的空闲空间偏移量设置为offset + raw.length
            setFSO(pg.getData(), (short)(offset+raw.length));
        }
    }

    // 将raw插入pg中的offset位置，不更新update
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        // 将pg的dirty标志设置为true，表示pg的数据已经被修改
        pg.setDirty(true);
        // 将raw的数据复制到pg的数据中的offset位置
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
