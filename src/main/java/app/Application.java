package app;

import example.SocketClientUsage;

public class Application {
    public static void main(String[] args) {
        // 启动监控脚本

        new Thread(() -> SocketClientUsage.main(args)).start();

        // 启动主业务逻辑

    }
}
