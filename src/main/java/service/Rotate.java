package service;

import model.command.CommandPos;

import java.io.FileWriter;
import java.io.IOException;

public class Rotate extends Thread {
    NormalStore normalStore;
    public Rotate (NormalStore normalStore) {
        this.normalStore = normalStore;
    }
    @Override
    public void run() {
        // 清空数据库文件
        normalStore.ClearFile("YY-db");

        // 压缩日志文件
        normalStore.CompressFile();

        // 重写数据库文件
        try (FileWriter writer = new FileWriter("YY-db")) {
            for (String key : normalStore.getIndex().keySet()) {
                writer.write(key + "," + normalStore.Get(key) + "\r\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}