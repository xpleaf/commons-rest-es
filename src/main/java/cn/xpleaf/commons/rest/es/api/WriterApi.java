package cn.xpleaf.commons.rest.es.api;

import cn.xpleaf.commons.rest.es.client.EsClient;
import cn.xpleaf.commons.rest.es.entity.EsDoc;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;

/**
 * @author xpleaf
 * @date 2019/1/5 10:49 PM
 *
 * 文档插入、更新
 */
public class WriterApi {

    private EsClient esClient;
    private String indexName;
    private String typeName;

    public WriterApi(EsClient esClient, String indexName, String typeName) {
        this.esClient = esClient;
        this.indexName = indexName;
        this.typeName = typeName;
    }

    /**
     * 实时插入一条文档
     * @param esDoc EsDoc对象，如果没有指定docId，则使用es内部生成的id
     */
    public boolean insertDoc(EsDoc esDoc) throws Exception {
        IndexRequest indexRequest = new IndexRequest(indexName, typeName);
        if(esDoc.getDocId() != null) {  // 是否使用用户自定义的id
            indexRequest.id(esDoc.getDocId());
        }
        indexRequest.source(esDoc.getDataMap());
        IndexResponse indexResponse = esClient.client.index(indexRequest);
        DocWriteResponse.Result result = indexResponse.getResult();
        // 如果存在相同id，则为更新操作
        return DocWriteResponse.Result.CREATED == result || DocWriteResponse.Result.UPDATED == result;
    }

}
