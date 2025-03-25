package top.yanjiazheng.mydb.backend.dm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.primitives.Bytes;

import top.yanjiazheng.mydb.backend.common.SubArray;
import top.yanjiazheng.mydb.backend.dm.dataItem.DataItem;
import top.yanjiazheng.mydb.backend.dm.logger.Logger;
import top.yanjiazheng.mydb.backend.dm.page.Page;
import top.yanjiazheng.mydb.backend.dm.page.PageX;
import top.yanjiazheng.mydb.backend.dm.pageCache.PageCache;
import top.yanjiazheng.mydb.backend.tm.TransactionManager;
import top.yanjiazheng.mydb.backend.utils.Panic;
import top.yanjiazheng.mydb.backend.utils.Parser;

public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    /**
     * 执行数据库恢复操作，包括重做和撤销事务
     * 此方法首先会重置日志文件，然后遍历所有日志条目，找出最大的页号，
     * 根据最大页号截断页缓存，之后执行重做和撤销事务操作，以确保数据一致性
     * 
     * @param tm TransactionManager实例，用于管理事务
     * @param lg Logger实例，用于日志记录和重放
     * @param pc PageCache实例，用于缓存和管理数据页
     */
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");
    
        // 重置日志文件，以便从头开始读取
        lg.rewind();
        int maxPgno = 0;
        // 遍历日志条目，找出最大的页号
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            int pgno;
            // 判断日志类型，解析并获取页号
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            // 更新最大页号
            if(pgno > maxPgno) {
                maxPgno = pgno;
            }
        }
        // 如果没有日志条目，将最大页号设为1
        if(maxPgno == 0) {
            maxPgno = 1;
        }
        // 根据最大页号截断页缓存
        pc.truncateByBgno(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");
    
        // 执行重做事务操作
        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transactions Over.");
    
        // 执行撤销事务操作
        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transactions Over.");
    
        System.out.println("Recovery Over.");
    }

    /**
     * 重做事务操作，用于系统恢复时重做未完成的事务
     * @param tm 事务管理器，用于检查事务是否处于活动状态
     * @param lg 日志对象，用于回放日志
     * @param pc 页面缓存对象，用于执行插入或更新操作
     */
    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        // 重置日志对象，以便从头开始读取日志
        lg.rewind();
        
        // 循环读取并处理每一条日志
        while(true) {
            // 读取下一条日志
            byte[] log = lg.next();
            
            // 如果没有更多日志，则退出循环
            if(log == null) break;
            
            // 判断日志类型是否为插入日志
            if(isInsertLog(log)) {
                // 解析插入日志信息
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                
                // 如果事务不再处于活动状态，则执行插入日志
                if(!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                // 解析更新日志信息
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                
                // 如果事务不再处于活动状态，则执行更新日志
                if(!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    /**
     * 回滚未完成的事务
     * 此方法通过日志文件来识别并回滚所有处于活动状态的事务到其初始状态
     * 
     * @param tm 事务管理器，用于检查事务是否处于活动状态
     * @param lg 日志管理器，用于重置并读取日志条目
     * @param pc 页缓存，用于执行插入和更新操作的回滚
     */
    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        // 用于缓存日志条目的Map，键为事务ID，值为该事务的所有日志条目列表
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        // 重置日志管理器到起始位置
        lg.rewind();
        // 循环读取日志条目，直到日志结束
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            // 判断日志类型并处理插入日志
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                // 如果事务处于活动状态，将其日志条目添加到缓存中
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                // 处理更新日志
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                // 如果事务处于活动状态，将其日志条目添加到缓存中
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }
    
        // 对所有active log进行倒序undo
        for(Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            // 倒序遍历日志条目并执行回滚操作
            for (int i = logs.size()-1; i >= 0; i --) {
                byte[] log = logs.get(i);
                // 根据日志类型执行相应的回滚操作
                if(isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            // 中止事务
            tm.abort(entry.getKey());
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE+1;
    private static final int OF_UPDATE_UID = OF_XID+8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8;

    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }

    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;
        short offset;
        byte[] raw;
        if(flag == REDO) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID+8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2;

    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(li.pgno);
        } catch(Exception e) {
            Panic.panic(e);
        }
        try {
            if(flag == UNDO) {
                DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }
}
