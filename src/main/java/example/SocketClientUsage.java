/*
 *@Type SocketClientUsage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 14:07
 * @version
 */
package org.yy.example;

import org.yy.client.Client;
import org.yy.client.CmdClient;
import org.yy.client.SocketClient;

public class SocketClientUsage {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 12345;
        Client client = new SocketClient(host, port);
        CmdClient cmdClient = new CmdClient(client);
        cmdClient.main();

        // 测试

//        client.set("zsy12","for test");
//        client.get("zsy12");
//        client.rm("zsy12");
//        client.get("zsy12");
//
//        client.get("zsy1");
//        client.get("zsy2");
//        client.get("zsy3");
//        client.get("zsy4");
    }
}