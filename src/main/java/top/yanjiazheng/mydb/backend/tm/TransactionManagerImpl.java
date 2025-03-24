package top.yanjiazheng.mydb.backend.tm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.yanjiazheng.mydb.backend.utils.Panic;
import top.yanjiazheng.mydb.backend.utils.Parser;
import top.yanjiazheng.mydb.common.Error;

public class TransactionManagerImpl implements TransactionManager {

    // XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE   = 0;
	private static final byte FIELD_TRAN_COMMITTED = 1;
	private static final byte FIELD_TRAN_ABORTED  = 2;

    // 超级事务，永远为commited状态
    public static final long SUPER_XID = 0;

    static final String XID_SUFFIX = ".xid";
    
    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter() {
        // 获取文件长度
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            // 如果文件长度获取失败，抛出异常
            Panic.panic(Error.BadXIDFileException);
        }
        // 如果文件长度小于XID文件头长度，抛出异常
        if(fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }
    
        // 读取XID文件头内容
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            // 如果文件读取失败，抛出异常
            Panic.panic(e);
        }
        // 解析并设置xidCounter的值
        this.xidCounter = Parser.parseLong(buf.array());
        // 计算并验证文件的理论长度与实际长度是否一致
        long end = getXidPosition(this.xidCounter + 1);
        if(end != fileLen) {
            // 如果不一致，抛出异常
            Panic.panic(Error.BadXIDFileException);
        }
    }

    // 根据事务xid取得其在xid文件中对应的位置
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid-1)*XID_FIELD_SIZE;
    }

    /**
     * 更新xid事务的状态为status
     * 
     * @param xid 事务的唯一标识符
     * @param status 事务的新状态
     */
    private void updateXID(long xid, byte status) {
        // 获取xid事务在文件中的位置
        long offset = getXidPosition(xid);
        // 创建一个用于存储xid事务状态的字节数组
        byte[] tmp = new byte[XID_FIELD_SIZE];
        // 将事务状态值写入字节数组的第一个位置
        tmp[0] = status;
        // 使用ByteBuffer包装字节数组以进行I/O操作
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            // 设置文件通道的位置到事务状态需要更新的位置
            fc.position(offset);
            // 将事务状态写入文件通道
            fc.write(buf);
        } catch (IOException e) {
            // 如果发生IO异常，触发系统panic
            Panic.panic(e);
        }
        try {
            // 确保文件通道的更改被持久化到存储设备中
            fc.force(false);
        } catch (IOException e) {
            // 如果发生IO异常，触发系统panic
            Panic.panic(e);
        }
    }

    // 将XID加一，并更新XID Header
    private void incrXIDCounter() {
        // 增加XID计数器
        xidCounter ++;
        // 将新的XID计数器转换为字节，并包装到ByteBuffer中
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            // 设置文件通道的位置为0，准备写入新的XID Header
            fc.position(0);
            // 将包含新XID计数器的ByteBuffer写入文件通道
            fc.write(buf);
        } catch (IOException e) {
            // 如果发生IO异常，则触发Panic
            Panic.panic(e);
        }
        try {
            // 强制将文件通道的内容写入存储设备
            fc.force(false);
        } catch (IOException e) {
            // 如果发生IO异常，则触发Panic
            Panic.panic(e);
        }
    }

    // 开始一个事务，并返回XID
    public long begin() {
        // 锁定计数器，以确保事务ID生成的线程安全性
        counterLock.lock();
        try {
            // 生成新的XID，通过当前XID计数加一得到
            long xid = xidCounter + 1;
            // 更新XID的状态为活跃事务
            updateXID(xid, FIELD_TRAN_ACTIVE);
            // 增加XID计数，为下一个事务做准备
            incrXIDCounter();
            // 返回新生成的XID
            return xid;
        } finally {
            // 无论try块中的执行结果如何，都解锁计数器
            counterLock.unlock();
        }
    }

    // 提交XID事务
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    // 回滚XID事务
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    // 检测XID事务是否处于status状态
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }   
        return buf.array()[0] == status;
    }

    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
