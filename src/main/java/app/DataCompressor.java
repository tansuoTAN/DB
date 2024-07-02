package app;

import java.io.*;
import java.nio.file.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class DataCompressor {
    private static final String DATA_FILE_PATH = "D:\\Desktop\\easy-db-main\\data.table";
    private static final String ZIP_DIR = "D:\\Desktop\\easy-db-main\\zip";
    private static final String OPERATION_COUNT_FILE = "D:\\Desktop\\easy-db-main\\operation_count.txt";
    private static final int THRESHOLD = 15;

    // 使用原子变量来记录操作次数，保证线程安全
    private AtomicInteger operationCount = new AtomicInteger(0);

    // 构造器，初始化操作计数和目录
    public DataCompressor() {
        // 确保zip目录存在
        File zipDir = new File(ZIP_DIR);
        if (!zipDir.exists()) {
            zipDir.mkdirs();
        }

        // 初始化操作计数
        try {
            if (Files.exists(Paths.get(OPERATION_COUNT_FILE))) {
                operationCount.set(readOperationCount());
            } else {
                writeOperationCount(0);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 主方法，用于启动监控
    public static void main(String[] args) {
        DataCompressor compressor = new DataCompressor();
        compressor.startMonitoring();
    }

    // 开始监控操作次数
    public void startMonitoring() {
        while (true) {
            try {
                Thread.sleep(10000); // 每10秒检查一次，可以根据需要调整频率
                checkAndCompress();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    // 检查操作次数并进行压缩
    private void checkAndCompress() {
        if (operationCount.get() >= THRESHOLD) {
            compressFile();
            operationCount.set(0);
            writeOperationCount(0);
        }
    }

    // 压缩文件的方法
    private void compressFile() {
        String zipFileName = ZIP_DIR + "\\data_" + System.currentTimeMillis() + ".zip";

        try (ZipOutputStream zos = new ZipOutputStream(new FileOutputStream(zipFileName));
             FileInputStream fis = new FileInputStream(DATA_FILE_PATH)) {

            ZipEntry zipEntry = new ZipEntry("data.table");
            zos.putNextEntry(zipEntry);

            byte[] buffer = new byte[1024];
            int len;
            while ((len = fis.read(buffer)) > 0) {
                zos.write(buffer, 0, len);
            }
            zos.closeEntry();
            System.out.println("Created zip: " + zipFileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 读取当前操作计数
    private int readOperationCount() throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(OPERATION_COUNT_FILE))) {
            return Integer.parseInt(reader.readLine().trim());
        }
    }

    // 写入操作计数
    private void writeOperationCount(int count) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(OPERATION_COUNT_FILE))) {
            writer.write(Integer.toString(count));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 增加操作计数
    public void incrementOperationCount() {
        int count = operationCount.incrementAndGet();
        writeOperationCount(count);
    }
}

