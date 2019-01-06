package cn.xpleaf.commons.rest.es.api;

import cn.xpleaf.commons.rest.es.client.EsClient;
import cn.xpleaf.commons.rest.es.entity.EsDoc;
import cn.xpleaf.commons.rest.es.enums.EsVersion;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.*;

/**
 * @author xpleaf
 * @date 2019/1/6 7:26 PM
 */
public class BulkWriterApiTest {

    // 直接在main方法中操作，因为内部的threadPool在test case下无法创建
    public static void main(String[] args) throws Exception {
        // 初始化连接客户端
        EsClient esClient = new EsClient.Builder()
                .setEsHosts("localhost:9200")
                .setEsVersion(EsVersion.V56)
                .build();
        BulkWriterApi writerApi = new BulkWriterApi.Builder(esClient, "bulk_index", "bulkType")
                .setBulkActions(1000)
                .setBulkSize(5)
                .setFlushInterval(10)
                .build();
        // 构建写入的数据
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("title", "Do you like elasticsearch and spark?");

        // 插入文档
        for (int i = 0; i < 20; i++) {
            writerApi.insertDoc(new EsDoc(i + "", dataMap));
        }
        writerApi.flush();

        // 等待一下，因为即便上面进行了flush操作，但是es内部仍然有可能还没有那么快更新，会导致下面的部分更新操作失败
        Thread.sleep(2000);

        // 更新文档
        dataMap.put("content", "Of cause I love es and spark, and you?");
        for (int i = 0; i < 10; i++) {
            writerApi.updateDoc(new EsDoc(i + "", dataMap));
        }

        Thread.sleep(2000);

        // 删除文档
        Random random = new Random();
        for (int i = 0; i < 5; i++) {
            writerApi.deleteDoc(random.nextInt(15) + "");
        }
        writerApi.flush();

        // 等待关闭
        writerApi.close();

    }

}