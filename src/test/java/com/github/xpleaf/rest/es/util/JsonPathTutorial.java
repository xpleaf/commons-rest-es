package com.github.xpleaf.rest.es.util;

import com.google.gson.Gson;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author xpleaf
 * @date 2019/1/5 5:09 PM
 *
 * JsonPath的经典使用案例，基于其开发的JsonPathUtil就是根据这些案例来开发的
 */
public class JsonPathTutorial {

    String json = "{\n" +
            "  \"size\" : 0,\n" +
            "  \"timeout\" : \"20000ms\",\n" +
            "  \"query\" : {\n" +
            "    \"bool\" : {\n" +
            "      \"must\" : [\n" +
            "        {\n" +
            "          \"range\" : {\n" +
            "            \"2713_收入\" : {\n" +
            "              \"from\" : 12000,\n" +
            "              \"to\" : null,\n" +
            "              \"include_lower\" : false,\n" +
            "              \"include_upper\" : true,\n" +
            "              \"boost\" : 1.0\n" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      ],\n" +
            "      \"disable_coord\" : false,\n" +
            "      \"adjust_pure_negative\" : true,\n" +
            "      \"boost\" : 1.0\n" +
            "    }\n" +
            "  }\n" +
            "}";

    // 1.读取
    @Test
    public void test01() throws Exception {
        DocumentContext documentContext = JsonPath.parse(json);
        String path = "$.query..include_lower";
        Object obj = documentContext.read(path);
        System.out.println(obj);
    }

    // 2.添加 "match_all":{}
    @Test
    public void test02() throws Exception {
        DocumentContext documentContext = JsonPath.parse(json);
        Map<String, Object> mapNode = new HashMap<>();
        mapNode.put("match_all", new HashMap<>());
        String path = "$.query..must";
        documentContext = documentContext.add(path, mapNode);
        String json = documentContext.jsonString();
        System.out.println(json);
    }

    // 3.删除 disable_coord、adjust_pure_negative、boost、include_lower、include_upper
    @Test
    public void test03() throws Exception {
        DocumentContext documentContext = JsonPath.parse(json);
        documentContext = documentContext
                .delete("$.query..disable_coord")
                .delete("$.query..adjust_pure_negative")
                .delete("$.query..boost")
                .delete("$.query..include_lower")
                .delete("$.query..include_upper");
        String json = documentContext.jsonString();
        System.out.println(json);
    }

    // 4.更新range查询
    @Test
    public void test04() throws Exception {
        DocumentContext documentContext = JsonPath.parse(json);
        String newRange = "{\n" +
                "    \"2713_收入\":{\n" +
                "        \"from\":12000,\n" +
                "        \"to\":20000\n" +
                "    }\n" +
                "}";
        documentContext = documentContext.set("$.query..range", new Gson().fromJson(newRange, Map.class));
        String json = documentContext.jsonString();
        System.out.println(json);
    }

    // 5.修改range为term（修改之后实际上在es中没有这样term后面加from to的语法，这里只是作为一种演示）
    @Test
    public void test05() throws Exception {
        DocumentContext documentContext = JsonPath.parse(json);
        Object rangeObj = documentContext.read("$.query.bool.must[0].range");
        Map<Object, Object> termMap = new HashMap<>();
        termMap.put("term", rangeObj);
        // 先删除range，再设置term，term中的内容就是range原来的内容，这样就做到了只替换"range"为"term"的目的
        documentContext = documentContext
                .delete("$.query.bool.must[0]")
                .add("$.query.bool.must", termMap);
        String json = documentContext.jsonString();
        System.out.println(json);
    }


    public static void main(String[] args) throws IOException {
        String json = "{\n" +
                "  \"size\" : 0,\n" +
                "  \"timeout\" : \"20000ms\",\n" +
                "  \"query\" : {\n" +
                "    \"bool\" : {\n" +
                "      \"must\" : [\n" +
                "        {\n" +
                "          \"range\" : {\n" +
                "            \"2713_收入\" : {\n" +
                "              \"from\" : 12000,\n" +
                "              \"to\" : null,\n" +
                "              \"include_lower\" : false,\n" +
                "              \"include_upper\" : true,\n" +
                "              \"boost\" : 1.0\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"disable_coord\" : false,\n" +
                "      \"adjust_pure_negative\" : true,\n" +
                "      \"boost\" : 1.0\n" +
                "    }\n" +
                "  }\n" +
                "}";

        DocumentContext documentContext = JsonPath.parse(json);
        // JsonPath p = JsonPath.compile("$.objs[0].obj");

        // 1.读取
        String path = "$.query..include_lower";
        Object obj = documentContext.read(path);
        System.out.println(obj);

        // 2.添加 "match_all":{}
        Map<String, Object> mapNode = new HashMap<>();
        mapNode.put("match_all", new HashMap<>());
        path = "$.query..must";
        DocumentContext documentContext1 = documentContext.add(path, mapNode);
        String json1 = documentContext1.jsonString();
        System.out.println(json1);

        // 3.删除 disable_coord、adjust_pure_negative、boost、include_lower、include_upper
        DocumentContext documentContext2 = documentContext
                .delete("$.query..disable_coord")
                .delete("$.query..adjust_pure_negative")
                .delete("$.query..boost")
                .delete("$.query..include_lower")
                .delete("$.query..include_upper");
        String json2 = documentContext2.jsonString();
        System.out.println(json2);

        // 4.更新range查询
        String newRange = "{\n" +
                "    \"2713_收入\":{\n" +
                "        \"from\":12000,\n" +
                "        \"to\":20000\n" +
                "    }\n" +
                "}";
        DocumentContext documentContext3 = documentContext.set("$.query..range", new Gson().fromJson(newRange, Map.class));
        String json3 = documentContext3.jsonString();
        System.out.println(json3);

        // 5.修改range为term（修改之后实际上在es中没有这样的语法，这里只是作为一种演示）
        Object rangeObj = documentContext.read("$.query.bool.must[0].range");
        Map<Object, Object> termMap = new HashMap<>();
        termMap.put("term", rangeObj);
        // 先删除range，再设置term，term中的内容就是range原来的内容，这样就做到了只替换"range"为"term"的目的
        DocumentContext documentContext4 = documentContext
                .delete("$.query.bool.must[0]")
                .add("$.query.bool.must", termMap);
        String json4 = documentContext4.jsonString();
        System.out.println(json4);

    }

}
