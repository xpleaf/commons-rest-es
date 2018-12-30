package cn.xpleaf.commons.rest.es.api;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * @author xpleaf
 * @date 2018/12/30 5:09 PM
 *
 * 索引操作，基于Jest API
 *
 * Jest API参考：
 * https://github.com/searchbox-io/Jest/tree/master/jest
 * 其索引操作功能正是es-high-level-client所缺的
 */
public class IndexApi {

    private static Logger LOG = LoggerFactory.getLogger(IndexApi.class);
    private JestClientFactory factory = new JestClientFactory();
    private JestClient client = null;


    /**
     * 多个es节点的构造方法
     * @param esHosts es节点列表，eg: {"192.168.10.101:9200", "192.168.10.102:9200"}
     */
    public IndexApi(String esHosts[]) {
        for (int i = 0; i < esHosts.length; i++) {
            // When use collection, it must include http:// in a host
            esHosts[i] = "http://" + esHosts[i];
        }
        init(esHosts);
    }

    /**
     * 单个es节点的构造方法
     * @param esHost es节点，eg: 192.168.10.101:9200
     */
    public IndexApi(String esHost) {
        init(new String[]{"http://" + esHost});
    }

    /**
     * factory和factory初始化
     * @param esHosts es节点列表，eg: {"192.168.10.101:9200", "192.168.10.102:9200"}
     */
    private void init(String esHosts[]) {
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(Arrays.asList(esHosts))
                .multiThreaded(true)
                //Per default this implementation will create no more than 2 concurrent connections per given route
                .defaultMaxTotalConnectionPerRoute(2)
                // and no more 20 connections in total
                .maxTotalConnection(10)
                .readTimeout(10 * 1000)
                .build());
        // JestClient is designed to be singleton, don't construct it for each request!
        client = factory.getObject();
    }


}
