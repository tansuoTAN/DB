/*
 *@Type Usage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 03:59
 * @version
 */
package example;

import service.NormalStore;

import java.io.File;

public class StoreUsage {
    public static void main(String[] args) {
        String dataDir="data"+ File.separator;
        NormalStore store = new NormalStore(dataDir);
        store.Set("zsy1","1");
        store.Set("zsy2","2");
        store.Set("zsy3","3");
        store.Set("zsy4","你好");
        System.out.println(store.Get("zsy4"));
        store.Remove("zsy4");
        System.out.println(store.Get("zsy4"));
    }
}