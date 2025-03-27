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
                // 如果当前事务已经提交，进行重做操作
                if(!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                // 如果是更新日志，解析日志记录，获取更新日志信息
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                
                // 如果当前事务已经提交，进行重做操作
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

    /**
     * 生成一个更新日志条目
     * 该方法用于创建一个更新操作的日志记录，其中包括事务ID、数据项的唯一标识、旧值和新值
     * 
     * @param xid 事务的唯一标识符
     * @param di 被更新的数据项
     * @return 返回一个字节数组，包含更新日志的所有必要信息
     */
    public static byte[] updateLog(long xid, DataItem di) {
        // 定义更新日志的类型
        byte[] logType = {LOG_TYPE_UPDATE};
        // 将事务ID转换为字节数组
        byte[] xidRaw = Parser.long2Byte(xid);
        // 将数据项的唯一标识转换为字节数组
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        // 获取数据项的旧值
        byte[] oldRaw = di.getOldRaw();
        // 获取数据项的新值
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        // 将所有部分合并成一个完整的日志条目
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    /**
     * 解析更新日志
     * 该方法负责解析给定的字节数组形式的更新日志，并将其映射到UpdateLogInfo对象中
     * 它提取日志中的xid（事务ID）、offset（偏移量）、pgno（页面编号）以及oldRaw和newRaw（旧数据和新数据）
     * 
     * @param log 字节数组，包含更新日志的信息
     * @return 解析后的UpdateLogInfo对象，包含提取的信息
     */
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        // 创建一个新的UpdateLogInfo对象来存储解析后的信息
        UpdateLogInfo li = new UpdateLogInfo();
        
        // 从日志中提取xid（事务ID）
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        
        // 从日志中提取uid，并进一步解析出offset（偏移量）和pgno（页面编号）
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));
        
        // 计算oldRaw和newRaw的长度，并据此从日志中提取oldRaw和newRaw
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        
        // 返回填充完毕的UpdateLogInfo对象
        return li;
    }

    /**
     * 执行更新日志操作
     * 根据提供的日志和标志，恢复或重做页面上的更新操作
     * 
     * @param pc 页缓存对象，用于访问和管理页面
     * @param log 更新日志数组，包含恢复或重做所需的信息
     * @param flag 标志位，指示是重做（REDO）还是回滚（UNDO）操作
     */
    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        // 定义页面编号和偏移量变量
        int pgno;
        short offset;
        byte[] raw;
        
        // 根据标志位决定是重做还是回滚操作
        if(flag == REDO) {
            // 解析更新日志，获取页面编号、偏移量和新的数据
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            // 解析更新日志，获取页面编号、偏移量和旧的数据
            UpdateLogInfo xi = parseUpdateLog(log);
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        
        // 初始化页面对象
        Page pg = null;
        try {
            // 从页缓存中获取指定编号的页面
            pg = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        
        // 执行页面更新恢复操作
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            // 释放页面资源，确保页面状态被正确更新
            pg.release();
        }
    }

    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID+8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2;

    /**
     * 创建插入日志
     * 该方法用于生成数据库操作中的插入日志，将操作信息序列化为字节数组，以便后续的重做日志应用
     * 
     * @param xid 事务ID，唯一标识一个事务
     * @param pg 页面对象，表示数据库中的一个数据页
     * @param raw 插入数据的原始字节数组，包含要插入的数据信息
     * @return 返回拼接后的日志字节数组，包含日志类型、事务ID、页号、偏移量和原始数据
     */
    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        // 日志类型字节数组，表示这是一个插入日志
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        // 将事务ID转换为字节数组
        byte[] xidRaw = Parser.long2Byte(xid);
        // 将页面号转换为字节数组
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        // 将页面中的空闲空间偏移量转换为字节数组
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        // 拼接所有字节数组，生成完整的插入日志
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    /**
     * 解析插入日志信息
     * 本方法从给定的字节数组中解析出插入日志的信息，并将其封装到InsertLogInfo对象中
     * 解析过程涉及从字节数组的不同部分提取交易ID、页号、偏移量和原始数据
     * 
     * @param log 字节数组，包含插入日志的信息
     * @return 返回一个InsertLogInfo对象，其中包含了解析出的日志信息
     */
    private static InsertLogInfo parseInsertLog(byte[] log) {
        // 创建一个InsertLogInfo对象来存储解析后的日志信息
        InsertLogInfo li = new InsertLogInfo();
        
        // 从log中提取交易ID(xid)，页号(pgno)，偏移量(offset)和原始数据(raw)
        // 使用Arrays.copyOfRange方法从字节数组中提取特定范围的字节，并将其转换为相应的数据类型
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        
        // 返回封装了日志信息的InsertLogInfo对象
        return li;
    }

    /**
     * 插入日志处理方法
     * 该方法负责将日志信息插入到指定的页面中，并根据标志决定是否进行撤销操作
     * 
     * @param pc 页缓存对象，用于访问和管理页面
     * @param log 日志数据，包含插入操作的相关信息
     * @param flag 操作标志，用于指示是否需要进行撤销操作
     */
    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        // 解析插入日志，获取日志信息对象
        InsertLogInfo li = parseInsertLog(log);
        // 初始化页面对象
        Page pg = null;
        // 尝试获取指定编号的页面
        try {
            pg = pc.getPage(li.pgno);
        } catch(Exception e) {
            Panic.panic(e);
        }
        // 执行插入日志的恢复操作
        try {
            // 如果标志为撤销操作
            if(flag == UNDO) {
                // 标记数据项为无效，准备撤销
                DataItem.setDataItemRawInvalid(li.raw);
            }
            // 调用页面恢复插入方法，根据日志信息恢复页面数据
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            // 释放页面资源，确保页面状态被正确更新
            pg.release();
        }
    }
}
