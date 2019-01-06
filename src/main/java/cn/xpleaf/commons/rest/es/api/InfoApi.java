package cn.xpleaf.commons.rest.es.api;

import cn.xpleaf.commons.rest.es.client.EsClient;
import org.elasticsearch.action.main.MainResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author xpleaf
 * @date 2019/1/6 8:46 PM
 *
 * 获取节点信息
 */
public class InfoApi {

    private static final Logger LOG = LoggerFactory.getLogger(IndexApi.class);
    private EsClient esClient;

    public InfoApi(EsClient esClient) {
        this.esClient = esClient;
    }

    // 测试连通性
    public boolean ping() {
        try {
            return esClient.client.ping();
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return false;
    }

    // 获取信息
    public MainResponse getMainInfo() {
        try {
            return esClient.client.info();
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
        return null;
    }

    // 关闭客户端
    public void close() {
        esClient.close();
    }
}
