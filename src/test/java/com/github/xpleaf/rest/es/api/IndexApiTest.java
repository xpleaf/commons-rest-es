package com.github.xpleaf.rest.es.api;

import com.github.xpleaf.rest.es.enums.SizeUnit;
import com.google.gson.Gson;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xpleaf
 * @date 2019/1/1 6:37 PM
 */
public class IndexApiTest {

    private IndexApi indexApi = null;

    @Before
    public void setUp() throws Exception {
        indexApi = new IndexApi("localhost:9200");
    }

    @Test
    public void indexExists() {
        System.out.println(indexApi.indexExists("my_index"));
    }

    @Test
    public void createIndex() {
        HashMap<Object, Object> settings = new HashMap<>();
        settings.put("number_of_shards", 3);
        settings.put("number_of_replicas", 1);
        System.out.println(indexApi.createIndex("my_index", settings));
    }

    @Test
    public void createType() {
        String mapping = "{\n" +
                "  \"properties\":{\n" +
                "    \"title\":{\n" +
                "      \"type\":\"keyword\"\n" +
                "    },\n" +
                "    \"content\":{\n" +
                "      \"type\":\"text\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        HashMap<Object, Object> settings = new HashMap<>();
        settings.put("number_of_shards", 3);
        settings.put("number_of_replicas", 1);
        System.out.println(indexApi.createType("my_index", "my_type", settings, mapping));
    }

    @Test
    public void createType1() {
        String mapping = "{\n" +
                "  \"properties\":{\n" +
                "    \"title\":{\n" +
                "      \"type\":\"keyword\"\n" +
                "    },\n" +
                "    \"content\":{\n" +
                "      \"type\":\"text\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        System.out.println(indexApi.createType("my_index", "my_type", mapping));
    }

    @Test
    public void createType2() {
        String mapping = "{\n" +
                "  \"properties\":{\n" +
                "    \"title\":{\n" +
                "      \"type\":\"keyword\"\n" +
                "    },\n" +
                "    \"content\":{\n" +
                "      \"type\":\"text\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        HashMap<Object, Object> settings = new HashMap<>();
        settings.put("number_of_shards", 3);
        settings.put("number_of_replicas", 1);
        Map mappingMap = new Gson().fromJson(mapping, Map.class);
        System.out.println(indexApi.createType("my_index", "my_type", settings, mappingMap));
    }

    @Test
    public void openIndex() {
        System.out.println(indexApi.openIndex("my_index"));
    }

    @Test
    public void closeIndex() {
        System.out.println(indexApi.closeIndex("my_index"));
    }

    @Test
    public void deleteIndex() {
        System.out.println(indexApi.deleteIndex("my_index"));
    }

    @Test
    public void indexList() {
        System.out.println(indexApi.indexList());
    }

    @Test
    public void getIndexByAlias() {
        System.out.println(indexApi.getIndexByAlias("alias_test"));
    }

    @Test
    public void getMapping() {
        System.out.println(indexApi.getMapping("my_index", "my_type"));
    }

    @Test
    public void indexSize() {
        System.out.println(indexApi.indexSize("my_index"));
    }

    @Test
    public void indexSize1() {
        System.out.println(indexApi.indexSize("my_index", SizeUnit.BYTES));
    }

    @After
    public void tearDown() throws Exception {
        indexApi.close();
    }
}