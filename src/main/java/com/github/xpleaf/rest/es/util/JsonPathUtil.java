package com.github.xpleaf.rest.es.util;

import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;

/**
 * @author xpleaf
 * @date 2019/1/5 12:47 AM
 *
 * Json操作
 * 基于JsonPath，参考：
 * https://github.com/json-path/JsonPath
 *
 * 需要注意JsonPathUtil add和put的区别
 * 1.add：往一个json数组添加元素，不需要指定key，直接添加就可以
 * 2.put：往一个json对象添加元素，需要指定key来添加元素
 */
public class JsonPathUtil {

    // 1.读取
    public static String read(String json, String path) {
        DocumentContext context = JsonPath.parse(json);
        Object obj = context.read(path);
        return obj.toString();
    }

    // 2.添加到数组
    public static String add(String json, String path, Object object) {
        DocumentContext context = JsonPath.parse(json);
        return context.add(path, object).jsonString();
    }

    // 3.添加到对象
    public static String put(String json, String path, String key, Object value) {
        DocumentContext context = JsonPath.parse(json);
        return context.put(path, key, value).jsonString();
    }

    // 4.更新
    public static String update(String json, String path, Object object) {
        DocumentContext context = JsonPath.parse(json);
        return context.set(path, object).jsonString();
    }

    // 5.删除
    public static String delete(String json, String ...paths) {
        DocumentContext context = JsonPath.parse(json);
        for(String path : paths) {
            context = context.delete(path);
        }
        return context.jsonString();
    }

}
