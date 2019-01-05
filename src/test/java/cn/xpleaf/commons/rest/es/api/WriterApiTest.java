package cn.xpleaf.commons.rest.es.api;

import cn.xpleaf.commons.rest.es.client.EsClient;
import cn.xpleaf.commons.rest.es.entity.EsDoc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author xpleaf
 * @date 2019/1/5 11:15 PM
 */
public class WriterApiTest {

    EsClient esClient;
    WriterApi writerApi;

    @Before
    public void init() throws Exception {
        esClient = new EsClient.Builder().setEsHosts("localhost:9200").build();
    }

    // 测试insertDoc方法，实时插入一条数据
    @Test
    public void test01() throws Exception {
        writerApi = new WriterApi(esClient, "bigdata", "stack");
        // 指定id
        Map<String, Object> dataMap = new HashMap<String, Object>(){{
            put("keyword", "elasticsearch");
            put("content", "do you like elasticsearch?");
        }};
        EsDoc esDoc = new EsDoc("1", dataMap);
        boolean isInsert = writerApi.insertDoc(esDoc);
        if(isInsert) {
            System.out.println("插入数据成功！插入的数据为：" + esDoc);
        }
        // 不指定id
        Map<String, Object> dataMap1 = new HashMap<String, Object>(){{
            put("keyword", "spark");
            put("content", "do you like spark?");
        }};
        EsDoc esDoc1 = new EsDoc(dataMap1);
        boolean isInsert1 = writerApi.insertDoc(esDoc1);
        if(isInsert1) {
            System.out.println("插入数据成功！插入的数据为：" + esDoc1);
        }
    }

    // 测试updateDoc方法，实时更新一条文档
    @Test
    public void test02() throws Exception {
        writerApi = new WriterApi(esClient, "bigdata", "stack");
        Map<String, Object> dataMap = new HashMap<String, Object>(){{
            put("content", "do you like es?");   // 修改
            put("tag", "es");                    // 新增字段
        }};
        EsDoc esDoc = new EsDoc("1", dataMap);
        boolean isUpdate = writerApi.updateDoc(esDoc);
        if(isUpdate) {
            System.out.println("更新文档成功！");
        }
    }

    // 测试updateDoc方法，upsert操作，更新或插入操作
    @Test
    public void test03() throws Exception {
        writerApi = new WriterApi(esClient, "bigdata", "stack");
        Map<String, Object> dataMap = new HashMap<String, Object>(){{
            put("keyword", "hadoop");
            put("content", "do you like hadoop?");
        }};
        EsDoc esDoc = new EsDoc("2", dataMap);
        boolean isUpdate = writerApi.updateDoc(esDoc, true);
        if(isUpdate) {
            System.out.println("更新或插入文档成功！");
        }
    }

    @After
    public void cleanUp() throws Exception {
        esClient.close();
    }

}