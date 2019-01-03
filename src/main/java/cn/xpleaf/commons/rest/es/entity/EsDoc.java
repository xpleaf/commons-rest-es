package cn.xpleaf.commons.rest.es.entity;

import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author xpleaf
 * @date 2019/1/3 10:48 PM
 */
public class EsDoc {

    private String docId;      // document的_id，注意这个是es内部的id
    private Map<String, Object> dataMap = new HashMap<>();  // document的数据部分

    // 用于构建写入es的doc，id由自己指定
    public EsDoc(String docId, Map<String, Object> dataMap) {
        this.docId = docId;
        this.dataMap = dataMap;
    }

    // 用于构建写入es的doc，id由es内部自动生成
    public EsDoc(Map<String, Object> dataMap) {
        this.dataMap = dataMap;
    }

    // 通过searchResponse的searchHit来构建EsDoc
    public static EsDoc genEsDoc(SearchHit hit) {
        return new EsDoc(hit.getId(), hit.getSourceAsMap());
    }

    // 通过searchResponse.getHits()的hits来构建EsDoc列表
    public static List<EsDoc> genEsDocList(SearchHits hits) {
        List<EsDoc> esDocList = new ArrayList<>();
        for(SearchHit hit : hits) {
            esDocList.add(genEsDoc(hit));
        }
        return esDocList;
    }

    public String getDocId() {
        return docId;
    }

    public void setDocId(String docId) {
        this.docId = docId;
    }

    public Map<String, Object> getDataMap() {
        return dataMap;
    }

    public void setDataMap(Map<String, Object> dataMap) {
        this.dataMap = dataMap;
    }

    @Override
    public String toString() {
        return "EsDoc{" +
                "docId='" + docId + '\'' +
                ", dataMap=" + "..." +
                '}';
    }
}
