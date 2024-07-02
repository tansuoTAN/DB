/*
 * @Type CmdClient.java
 * @Desc 这是一个命令行客户端类，用于与数据库进行交互
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 13:58
 * @version
 */
package client;

import org.apache.commons.cli.*;

import java.util.Arrays;
import java.util.Scanner;

public class CmdClient{
    // 客户端实例，用于与数据库交互
    private Client client;

    // 无参构造器
    public CmdClient() {}

    // 带参构造器，接收一个Client实例
    public CmdClient(Client client) {
        this.client = client;
    }

    // 主方法，用于启动命令行客户端
    public void main(){
        // 创建Scanner对象，用于从标准输入读取数据
        Scanner scanner = new Scanner(System.in);
        // 创建字符串数组，用于存储输入的命令
        String[] input = new String[1];
        // 无限循环，等待用户输入命令
        while (true){
            // 读取用户输入的一行文本
            input[0] =  scanner.nextLine();
            // 处理命令
            CMD(input);
        }
    }

    // 处理命令的方法
    public void CMD(String[] input) {
        // 创建 Options 对象，用于存储所有命令行选项
        Options options = new Options();

        // 添加 -s|--set 选项，它接受两个参数（键和值）
        Option setOption = Option.builder("s")
                .longOpt("set")
                .hasArg()
                .numberOfArgs(2) // 选项接受两个参数
                .argName("key value") // 参数名称提示
                .desc("设置键值对") // 选项描述
                .build();
        options.addOption(setOption);

        // 添加 -g|--get 选项，它接受一个参数（键）
        Option getOption = Option.builder("g")
                .longOpt("get")
                .hasArg()
                .argName("key")
                .desc("获取指定键的值")
                .build();
        options.addOption(getOption);

        // 添加 -r|--remove 选项，它接受一个参数（键）
        Option rmOption = Option.builder("r")
                .longOpt("remove")
                .hasArg()
                .argName("key")
                .desc("删除指定键的键值对")
                .build();
        options.addOption(rmOption);

        // 添加 -h|--help 选项，用于显示帮助信息
        Option helpOption = Option.builder("h")
                .longOpt("help")
                .desc("显示此帮助信息")
                .build();
        options.addOption(helpOption);

        // 创建 HelpFormatter 对象，用于格式化和打印帮助信息
        HelpFormatter formatter = new HelpFormatter();

        // 创建 DefaultParser 对象，用于解析命令行参数
        CommandLineParser parser = new DefaultParser();

        // 解析命令行参数，并将结果存储在 CommandLine 对象 cmd 中
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, input);
        } catch (ParseException e) {
            // 解析失败，打印错误信息和帮助信息，并退出程序
            System.err.println("命令解析失败。原因: " + e.getMessage());
            formatter.printHelp("DB", options);
            System.exit(1);
        }

        // 打印解析后的选项，主要用于调试
        System.out.println(Arrays.toString(cmd.getOptions()));

        // 根据解析结果执行操作
        if (cmd.hasOption("s")) {
            // 获取并分割设置选项的值
            String[] setArgs = cmd.getOptionValue("s").split(" ");
            String key = setArgs[0];
            String value = setArgs[1];
            client.Set(key, value); // 调用Client的Set方法设置键值对
        } else if (cmd.hasOption("g")) {
            // 获取获取选项的值
            String key = cmd.getOptionValue("g"); // 获取键
            client.Get(key); // 调用Client的Get方法获取键对应的值
        } else if (cmd.hasOption("r")) {
            // 获取删除选项的值
            String key = cmd.getOptionValue("r"); // 获取键
            client.Remove(key); // 调用Client的Remove方法删除键值对
        } else if (cmd.hasOption("h")) {
            // 打印帮助信息
            PrintHelp(formatter, options);
        } else {
            // 如果输入的选项不是预定义的，则打印帮助信息
            PrintHelp(formatter, options);
        }
    }

    // 打印帮助信息的方法
    private static void PrintHelp(HelpFormatter formatter, Options options) {
        // 打印命令行客户端的帮助信息
        formatter.printHelp("DB", options, true);
    }

    // 测试方法，暂时没有实现任何功能
    public void Test(String[] input) {
        // 此方法尚未实现任何功能
    }
}

// 注意：Client类没有在此代码片段中提供，它应该是实现数据库交互的实际逻辑的地方。
