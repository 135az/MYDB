package top.yanjiazheng.mydb.backend.dm.logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.primitives.Bytes;

import top.yanjiazheng.mydb.backend.utils.Panic;
import top.yanjiazheng.mydb.backend.utils.Parser;
import top.yanjiazheng.mydb.common.Error;

/**
 * 日志文件读写
 * 
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 * 
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 */
public class LoggerImpl implements Logger {

    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;
    
    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    /**
     * 初始化方法，用于初始化日志文件的元数据
     * 该方法首先确定日志文件的大小，然后读取并验证日志文件的头部信息，
     * 包括读取校验和，并将相关值存储到实例变量中
     */
    void init() {
        long size = 0;
        try {
            // 获取日志文件的大小
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 如果文件大小小于4字节，表示文件不合法，则触发特定异常
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }
    
        // 分配一个4字节的缓冲区，用于读取校验和
        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            // 设置文件通道的位置到文件开始处
            fc.position(0);
            // 从文件通道读取数据到缓冲区
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 解析并获取校验和
        int xChecksum = Parser.parseInt(raw.array());
        // 将日志文件的大小和校验和保存到实例变量中
        this.fileSize = size;
        this.xChecksum = xChecksum;
    
        // 调用方法检查并移除日志文件的尾部无效数据
        checkAndRemoveTail();
    }

    // 检查并移除bad tail
    private void checkAndRemoveTail() {
        // 重置文件指针到文件头
        rewind();
    
        // 初始化校验和变量
        int xCheck = 0;
        // 循环读取日志直到文件末尾
        while(true) {
            // 读取下一个日志项
            byte[] log = internNext();
            // 如果日志项为空，跳出循环
            if(log == null) break;
            // 更新校验和
            xCheck = calChecksum(xCheck, log);
        }
        // 如果校验和不匹配，抛出异常
        if(xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }
    
        // 尝试截断文件到当前position位置
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        // 尝试将文件指针移动到当前position位置
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 重置文件指针到文件头
        rewind();
    }

    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 重写log方法以记录日志数据
     * 该方法首先将输入数据进行预处理，然后加锁以确保线程安全，
     * 将处理后的数据追加到日志文件的末尾，最后释放锁并更新校验和
     * 
     * @param data 要记录的日志数据，类型为byte数组
     */
    @Override
    public void log(byte[] data) {
        // 对日志数据进行预处理
        byte[] log = wrapLog(data);
        // 创建ByteBuffer对象以方便写入日志数据
        ByteBuffer buf = ByteBuffer.wrap(log);
        // 加锁以确保线程安全
        lock.lock();
        try {
            // 将文件通道的位置移动到文件末尾
            fc.position(fc.size());
            // 将日志数据写入文件
            fc.write(buf);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            // 释放锁，以确保其他线程可以进行日志记录
            lock.unlock();
        }
        // 更新校验和，以确保数据完整性
        updateXChecksum(log);
    }

    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 读取并返回下一个内部日志条目
     * 该方法首先检查当前文件位置是否接近文件末尾，如果是，则返回null
     * 然后，它尝试读取一个表示日志条目长度的整数如果读取成功，
     * 它会根据这个长度分配一个缓冲区，并读取整个日志条目
     * 最后，它计算并验证日志条目的校验和如果校验和匹配，
     * 它会更新文件位置并返回日志条目；否则，返回null
     * 
     * @return 下一个日志条目，如果到达文件末尾或校验和不匹配，则返回null
     */
    private byte[] internNext() {
        // 检查当前文件位置是否接近文件末尾
        if(position + OF_DATA >= fileSize) {
            return null;
        }
        // 分配一个临时ByteBuffer用于读取日志条目的长度
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            // 设置文件通道的位置并读取日志条目长度到临时缓冲区
            fc.position(position);
            fc.read(tmp);
        } catch(IOException e) {
            Panic.panic(e);
        }
        // 解析日志条目的长度
        int size = Parser.parseInt(tmp.array());
        // 检查日志条目是否超出文件末尾
        if(position + size + OF_DATA > fileSize) {
            return null;
        }
    
        // 分配一个缓冲区用于读取整个日志条目
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            // 重新设置文件通道的位置并读取整个日志条目到缓冲区
            fc.position(position);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }
    
        // 获取日志条目作为字节数组
        byte[] log = buf.array();
        // 计算日志条目的校验和
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        // 从日志条目中解析存储的校验和
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        // 比较计算的校验和与存储的校验和
        if(checkSum1 != checkSum2) {
            return null;
        }
        // 更新文件位置
        position += log.length;
        // 返回日志条目
        return log;
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
    
}
