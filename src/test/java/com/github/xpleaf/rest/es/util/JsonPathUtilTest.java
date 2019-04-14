package com.github.xpleaf.rest.es.util;

import com.google.gson.Gson;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xpleaf
 * @date 2019/1/5 5:46 PM
 *
 * 需要注意JsonPathUtil add和put的区别
 * 1.add：往一个json数组添加元素，不需要指定key，直接添加就可以
 * 2.put：往一个json对象添加元素，需要指定key来添加元素
 */
public class JsonPathUtilTest {

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

    @Test
    public void read() {
        String result = JsonPathUtil.read(json, "$.query.bool.must[0].range");
        System.out.println(result);
    }

    @Test
    public void add() {
        Map<String, Object> mapNode = new HashMap<>();
        mapNode.put("match_all", new HashMap<>());
        String result = JsonPathUtil.add(json, "$.query.bool.must", mapNode);
        System.out.println(result);
    }

    @Test
    public void update() {
        String result = JsonPathUtil.update(json, "$.query..include_lower", true);
        System.out.println(result);
    }

    @Test
    public void delete() {
        String result = JsonPathUtil.delete(json, "$.query..include_lower", "$.query..include_upper", "$.query..boost");
        System.out.println(result);
    }

    @Test
    public void complexTest() {
        /**
         * 下面的查询是es 5.6的查询语法，es 1.7的大致相同，但是其function_score下面的query，
         * 不叫query，而是叫做filter，所以只需要把"query"替换成"filter"即可
         */
        json = "{\n" +
                "  \"size\": 100, \n" +
                "  \"query\": {\n" +
                "    \"function_score\": {\n" +
                "      \"query\": {\n" +
                "        \"bool\": {\n" +
                "          \"must\": [\n" +
                "            {\n" +
                "              \"range\": {\n" +
                "                \"publish_time\": {\n" +
                "                  \"from\": \"1541952000262\",\n" +
                "                  \"to\": \"1542023525262\"\n" +
                "                }\n" +
                "              }\n" +
                "            },\n" +
                "            {\n" +
                "              \"exists\":{\n" +
                "                \"field\":\"topics\"\n" +
                "              }\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      },\n" +
                "      \"functions\": [\n" +
                "        {\n" +
                "          \"random_score\": {\"seed\": \"1542023525262\"}\n" +
                "          \n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"_source\": [ \"uid\", \"publish_time\", \"data_type\", \"content\", \"url\", \"pic_urls\", \"topics\" ]\n" +
                "}";
        // 先读取query.function_score.query的内容
        String query = JsonPathUtil.read(json, "$.query.function_score.query");
        // 再删除query.function_score.query
        json = JsonPathUtil.delete(this.json, "$.query.function_score.query");
        // 再添加query.function_score.filter
        json = JsonPathUtil.put(this.json, "$.query.function_score", "filter", new Gson().fromJson(query, Map.class));

        System.out.println(json);
    }
}