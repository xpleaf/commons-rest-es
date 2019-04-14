package com.github.xpleaf.rest.es.util;

import com.github.xpleaf.rest.es.api.BulkWriterApi;
import com.github.xpleaf.rest.es.api.IndexApi;
import com.github.xpleaf.rest.es.api.ReaderApi;
import com.github.xpleaf.rest.es.api.WriterApi;
import com.github.xpleaf.rest.es.client.EsClient;
import com.github.xpleaf.rest.es.entity.EsDoc;
import com.github.xpleaf.rest.es.entity.EsReaderResult;
import com.github.xpleaf.rest.es.enums.EsVersion;
import com.google.gson.Gson;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xpleaf
 * @date 2019/1/8 10:45 AM
 */
public class LoadDataTest {

    private static EsClient esClient1;
    private static EsClient esClient2;
    private static IndexApi indexApi1;
    private static IndexApi indexApi2;
    private static ReaderApi readerApi;
    private static WriterApi writerApi;
    private static BulkWriterApi bulkWriterApi;

    private static String indexName = "indexName";
    private static String typeName = "typeName";

    @Before
    public void init() throws Exception {
        esClient1 = new EsClient.Builder()
                .setEsHosts("es01:9200")
                .setEsVersion(EsVersion.V56)
                .build();
        esClient2 = new EsClient.Builder()
                .setEsHosts("localhost:9200")
                .setEsVersion(EsVersion.V56)
                .build();
        indexApi1 = new IndexApi("es01:9200");
        indexApi2 = new IndexApi("localhost:9200");;
        readerApi = new ReaderApi(esClient1)
                .setIndexName(indexName)
                .setTypeName(typeName);
        writerApi = new WriterApi(esClient2, indexName, typeName);
    }

    // 索引操作
    @Test
    public void test01() throws Exception {
        // 创建索引
        Map<Object, Object> settings = new HashMap();
        settings.put("number_of_shards", 3);
        settings.put("number_of_replicas", 1);
        boolean isCreated = indexApi2.createIndex(indexName, settings);

        System.out.println(isCreated);
    }

    // 类型操作
    @Test
    public void test02() throws Exception {
        Map<String, String> mapping = indexApi1.getMapping(indexName, typeName);
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("properties", mapping);
        // 创建类型
        String fullMappings = new Gson().toJson(properties);
        // 将分词器替换掉，因为words这个分词器在我的es中没有，只有ik_max_word
        fullMappings = fullMappings.replaceAll("\"analyzer\":\"words\"", "\"analyzer\":\"ik_max_word\"");
        boolean isCreated = indexApi2.createType(indexName, typeName, fullMappings);

        System.out.println(isCreated);
    }

    // 数据读取与写入
    @Test
    public void test03() throws Exception {
        EsReaderResult esReaderResult = readerApi.search(null, 1000, QueryBuilders.matchAllQuery(), null, null);
        for(EsDoc esDoc : esReaderResult.getEsDocList()) {
            System.out.println(esDoc.getDataMap());
            boolean isInserted = writerApi.insertDoc(esDoc);
            System.out.println(isInserted);
        }
        Thread.sleep(5000);
    }

    public static void main(String[] args) throws Exception {
        esClient1 = new EsClient.Builder()
                .setEsHosts("es01:9200")
                .setEsVersion(EsVersion.V56)
                .build();
        esClient2 = new EsClient.Builder()
                .setEsHosts("localhost:9200")
                .setEsVersion(EsVersion.V56)
                .build();
        indexApi1 = new IndexApi("es01:9200");
        indexApi2 = new IndexApi("localhost:9200");;
        readerApi = new ReaderApi(esClient1)
                .setIndexName(indexName)
                .setTypeName(typeName);
        writerApi = new WriterApi(esClient2, indexName, typeName);
        bulkWriterApi = new BulkWriterApi
                .Builder(esClient2, indexName, typeName)
                .setBulkActions(10000)
                .setBulkSize(100)
                .build();

        EsReaderResult esReaderResult = readerApi.scroll(10000, QueryBuilders.matchAllQuery(), null, null, 300 * 1000L);

        int count = 0;
        while (esReaderResult.getEsDocList().size() > 0) {
            count += esReaderResult.getEsDocList().size();
            // 写入数据
            for(EsDoc esDoc : esReaderResult.getEsDocList()) {
                bulkWriterApi.insertDoc(esDoc);
            }
            bulkWriterApi.flush();
            Thread.sleep(3000);

            // 获取下一批数据
            esReaderResult = readerApi.scroll(esReaderResult.getScrollId(), 300 * 1000L);
            System.out.println("count: " + count);
        }

        bulkWriterApi.close();
        esClient1.close();
        esClient2.close();
        System.out.println(count);

    }


}
