package cn.xpleaf.commons.rest.es.api;

import cn.xpleaf.commons.rest.es.client.EsClient;
import cn.xpleaf.commons.rest.es.entity.EsDoc;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;

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

    /**
     * 实时更新一条文档
     * Note：为局部更新，对于存在的document，可以添加字段或修改已有字段的内容
     * @param esDoc esDoc必须存在id
     */
    public boolean updateDoc(EsDoc esDoc) throws Exception {
        if(esDoc.getDocId() == null) {
            throw new Exception("docId不能为空！");
        }
        // 更新操作时，如果在es中指定id的文档不存在，是会抛出异常的
        UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, esDoc.getDocId());
        updateRequest.doc(esDoc.getDataMap());
        UpdateResponse updateResponse = esClient.client.update(updateRequest);
        DocWriteResponse.Result result = updateResponse.getResult();

        // NOOP，什么操作也没做，也处理为更新成功，此时说明要执行的更新操作跟document已经存在的内容是一样的
        return DocWriteResponse.Result.UPDATED == result || DocWriteResponse.Result.NOOP == result;
    }

    /**
     * 实时更新一条文档，upsert操作
     * Note：为局部更新，对于存在的document，可以添加字段或修改已有字段的内容
     * @param esDoc     esDoc必须存在id
     * @param upsert    upsert为true时，如果document存在，则更新，不存在则写入该文档
     *                  upsert为false时，执行普通的更新操作
     */
    public boolean updateDoc(EsDoc esDoc, boolean upsert) throws Exception {
        if(esDoc.getDocId() == null) {
            throw new Exception("docId不能为空！");
        }
        if(!upsert) {   // 普通的更新操作
            return updateDoc(esDoc);
        }
        // 执行upsert操作
        UpdateRequest updateRequest = new UpdateRequest(indexName, typeName, esDoc.getDocId());
        updateRequest.upsert(esDoc.getDataMap());
        updateRequest.doc(esDoc.getDataMap());
        UpdateResponse updateResponse = esClient.client.update(updateRequest);
        DocWriteResponse.Result result = updateResponse.getResult();

        // NOOP，什么操作也没做，也处理为更新成功，此时说明要执行的更新操作跟document已经存在的内容是一样的
        return DocWriteResponse.Result.CREATED == result ||
               DocWriteResponse.Result.UPDATED == result ||
               DocWriteResponse.Result.NOOP == result;
    }

    /**
     * 实时删除一条文档
     * @param docId 需要删除的文档id，即es内部的_id
     */
    public boolean deleteDoc(String docId) throws Exception {
        if(docId == null) {
            throw new Exception("docId不能为空！");
        }
        DeleteRequest deleteRequest = new DeleteRequest(indexName, typeName, docId);
        DeleteResponse deleteResponse = esClient.client.delete(deleteRequest);

        // 为DocWriteResponse.Result.NOT_FOUND，表示不存在，这时删除失败
        return DocWriteResponse.Result.DELETED == deleteResponse.getResult();
    }

}
