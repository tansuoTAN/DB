/*
 *@Type NormalStore.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 02:07
 * @version
 */
package service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Getter;
import model.command.Command;
import model.command.CommandPos;
import model.command.RmCommand;
import model.command.SetCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.CommandUtil;
import utils.LoggerUtil;
import utils.RandomAccessFileUtil;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class NormalStore implements Store {

    // 常量，表示数据表的文件扩展名
    public static final String TABLE = ".table";
    // 常量，表示文件的读写模式
    public static final String RW_MODE = "rw";
    // 常量，表示数据文件的名称
    public static final String NAME = "data";
    // 日志记录器，用于记录日志
    private final Logger LOGGER = LoggerFactory.getLogger(NormalStore.class);
    // 日志格式模板
    private final String logFormat = "[NormalStore][{}]: {}";

    /**
     * 内存表，类似缓存，用于存储键值对
     */
    private TreeMap<String, Command> memTable;

    /**
     * 哈希索引，存储的是数据长度和偏移量，用于快速定位数据
     */
    @Getter
    private HashMap<String, CommandPos> index;

    /**
     * 数据目录，用于存储数据文件的路径
     */
    private final String dataDir;

    /**
     * 读写锁，支持多线程，并发安全写入，用于控制对索引的访问
     */
    private final ReadWriteLock indexLock;

    /**
     * 暂存数据的日志句柄，用于写入和读取数据文件
     */
    private RandomAccessFile writerReader;

    /**
     * 持久化阈值，当内存表中的操作次数达到此值时，将数据写入磁盘
     */
    private final int storeThreshold = 5;

    /**
     * 计数器，用于记录内存表中的操作次数
     */
    private int storeOperateNumber;

    // 构造器，接收数据目录作为参数
    public NormalStore(String dataDir) {
        this.dataDir = dataDir;
        this.indexLock = new ReentrantReadWriteLock();
        this.memTable = new TreeMap<String, Command>();
        this.index = new HashMap<>();
        storeOperateNumber = 0;

        // 检查数据目录是否存在，如果不存在则创建
        File file = new File(dataDir);
        if (!file.exists()) {
            LoggerUtil.info(LOGGER, logFormat, "NormalStore", "dataDir isn't exist,creating...");
            file.mkdirs();
        }
        // 从磁盘重新加载索引
        this.reloadIndex();
    }

    // 生成数据文件的路径
    public String genFilePath() {
        return this.dataDir + File.separator + NAME + TABLE;
    }

    // 从磁盘重新加载索引
    public void reloadIndex() {
        try {
            // 打开数据文件
            RandomAccessFile file = new RandomAccessFile(this.genFilePath(), RW_MODE);
            long len = file.length();
            long start = 0;
            file.seek(start);
            // 遍历文件，解析命令并更新索引
            while (start < len) {
                int cmdLen = file.readInt();
                byte[] bytes = new byte[cmdLen];
                file.read(bytes);
                JSONObject value = JSON.parseObject(new String(bytes, StandardCharsets.UTF_8));
                Command command = CommandUtil.jsonToCommand(value);
                start += 4;
                if (command != null) {
                    CommandPos cmdPos = new CommandPos((int) start, cmdLen);
                    index.put(command.getKey(), cmdPos);
                    // 如果命令是删除命令，则从内存表和索引中移除键值对
                    if (command.getClass().equals(RmCommand.class)) {
                        index.remove(command.getKey(), cmdPos);
                    }
                }
                start += cmdLen;
            }
            file.seek(file.length());
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 记录索引的当前状态
        LoggerUtil.debug(LOGGER, logFormat, "reload index: " + index.toString());
    }

    // 实现Store接口的Set方法，用于设置键值对
    @Override
    public void Set(String key, String value) {
        try {
            // 创建Set命令
            SetCommand command = new SetCommand(key, value);
            byte[] commandBytes = command.toByte();
            // 加写锁，确保并发安全
            indexLock.writeLock().lock();
            // 如果内存表的操作次数达到阈值，则写入磁盘
            if (storeOperateNumber >= storeThreshold) {
                storeOperateNumber = 0;
                // TODO: RotateDataBaseFile() 方法未实现，可能是用于文件轮转的逻辑
                RotateFile(this);
            }
            // 写入命令到数据文件
            RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
            int pos = RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
            // 更新索引
            CommandPos cmdPos = new CommandPos(pos, commandBytes.length);
            index.put(key, cmdPos);
            storeOperateNumber++;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            // 释放写锁
            indexLock.writeLock().unlock();
        }
    }

    // 实现Store接口的Get方法，用于获取键对应的值
    @Override
    public String Get(String key) {
        try {
            // 加读锁，确保并发安全
            indexLock.readLock().lock();
            // 从索引中获取键的位置信息
            CommandPos cmdPos = index.get(key);
            if (cmdPos == null) {
                return null;
            }
            // 从数据文件中读取命令数据
            byte[] commandBytes = RandomAccessFileUtil.readByIndex(this.genFilePath(), cmdPos.getPos(), cmdPos.getLen());
            JSONObject value = null;
            if (commandBytes != null) {
                value = JSONObject.parseObject(new String(commandBytes));
            }
            Command cmd = null;
            if (value != null) {
                cmd = CommandUtil.jsonToCommand(value);
            }
            // 根据命令类型返回值或null
            if (cmd instanceof SetCommand) {
                return ((SetCommand) cmd).getValue();
            }
            if (cmd instanceof RmCommand) {
                return null;
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            // 释放读锁
            indexLock.readLock().unlock();
        }
        return null;
    }

    // 实现Store接口的Remove方法，用于删除键值对
    @Override
    public void Remove(String key) {
        try {
            // 创建Remove命令
            RmCommand command = new RmCommand(key);
            byte[] commandBytes = command.toByte();
            // 加写锁，确保并发安全
            indexLock.writeLock().lock();
            // 如果内存表的操作次数达到阈值，则写入磁盘
            if (storeOperateNumber >= storeThreshold) {
                storeOperateNumber = 0;
                // TODO: RotateDataBaseFile() 方法未实现，可能是用于文件轮转的逻辑
                RotateFile(this);
            }
            // 写入命令到数据文件
            RandomAccessFileUtil.writeInt(this.genFilePath(), commandBytes.length);
            RandomAccessFileUtil.write(this.genFilePath(), commandBytes);
            // 从索引中移除键值对
            index.remove(key);
            storeOperateNumber++;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            // 释放写锁
            indexLock.writeLock().unlock();
        }
    }

    // 实现Store接口的ReDoLog方法，用于重新加载索引和执行文件轮转
    @Override
    public void ReDoLog() {
        reloadIndex();
        // TODO: RotateDataBaseFile() 方法未实现，可能是用于文件轮转的逻辑
        RotateFile(this);
    }

    // 实现Store接口的close方法，用于关闭资源
    @Override
    public void close() {
        // TODO: 实现资源关闭的逻辑
    }

    // 清空文件内容
    public void ClearFile(String filePath) {
        try (FileWriter writer = new FileWriter(filePath)) {
            // 不写入任何内容，直接关闭writer，清空文件
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 压缩索引，移除重复数据并重写文件
    public void CompressFile() {
        ArrayList<String> arrayList = new ArrayList<>();
        HashSet<String> hashSet = new HashSet<>();

        try (Scanner scanner = new Scanner(new File(this.genFilePath()))) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                // 处理每一行，避免重复
                if (!hashSet.contains(line)) {
                    arrayList.add(line);
                    hashSet.add(line);
                }
            }
            //arrayList.remove(arrayList.size() - 1);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        ClearFile(this.genFilePath());

        try (FileWriter writer = new FileWriter(this.genFilePath())) {
            for (String line : arrayList) {
                writer.write(line + "\r\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 清除索引
        index.clear();

        // 重新加载索引
        reloadIndex();
    }

    // 执行文件轮转，可能是用于处理日志文件或数据文件的增长
    public void RotateFile(NormalStore normalStore) {
        Rotate rotate = new Rotate(normalStore);
        rotate.start();

    }
}
