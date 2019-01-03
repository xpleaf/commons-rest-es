package cn.xpleaf.commons.rest.es.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author xpleaf
 * @date 2019/1/2 11:27 AM
 *
 * ReaderApi、WriterApi、InfoApi的连接客户端，基于RestHighLevelClient
 * 其关系为：
 * EsClient -> RestHighLevelClient -> RestClient(rest-low-level-client)
 */
public class EsClient {

    private static Logger LOG = LoggerFactory.getLogger(EsClient.class);

    // rest-low-level-client
    private RestClient restLowLevelClient;
    // rest-high-level-client，protected，ReaderApi、WriterApi、InfoApi需要依赖其与es节点建立连接
    public RestHighLevelClient client;

    private EsClient(RestClient restLowLevelClient, RestHighLevelClient client) {
        this.restLowLevelClient = restLowLevelClient;
        this.client = client;
    }

    // 关闭客户端连接，restLowLevelClient是需要手动关闭的，不然线程会一直处于等待状态
    public void close() {
        if (restLowLevelClient != null) {
            try {
                restLowLevelClient.close();
            } catch (IOException e) {
                LOG.error("关闭客户端连接失败，原因为：{}", e.getMessage());
            }
        }
    }

    // EsClient对外builder
    public static class Builder {

        // es节点地址数组，eg: 192.168.10.101:9200
        private String[] esHosts;

        // 传入单个esHost或者多个，或者包含其的数组都可以
        public Builder setEsHosts(String... esHosts) {
            this.esHosts = esHosts;
            return this;
        }

        public EsClient build() throws Exception {
            RestClient restLowLevelClient = RestClient.builder(buildHttpHosts(this.esHosts)).build();
            RestHighLevelClient client = new RestHighLevelClient(restLowLevelClient);
            return new EsClient(restLowLevelClient, client);
        }

        // 构建HttpHost对象数组，需要作为参数传入restLowLevelClient
        private static HttpHost[] buildHttpHosts(String[] esHosts) throws Exception {
            HttpHost[] httpHosts = new HttpHost[esHosts.length];
            try {
                for (int i = 0; i < esHosts.length; i++) {
                    String[] params = esHosts[i].split(":");
                    httpHosts[i] = new HttpHost(params[0], Integer.valueOf(params[1]), "http");
                }
            } catch (Exception e) {
                throw new Exception("es地址格式有误，解析失败：" + e.getMessage());
            }
            return httpHosts;
        }


    }

}
