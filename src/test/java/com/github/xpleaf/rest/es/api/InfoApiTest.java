package com.github.xpleaf.rest.es.api;

import com.github.xpleaf.rest.es.client.EsClient;
import com.github.xpleaf.rest.es.enums.EsVersion;
import org.elasticsearch.action.main.MainResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author xpleaf
 * @date 2019/1/6 9:01 PM
 */
public class InfoApiTest {

    EsClient esClient;
    InfoApi infoApi;

    @Before
    public void init() throws Exception {
        esClient = new EsClient.Builder()
                .setEsHosts("localhost:9200")
                .setEsVersion(EsVersion.V56)
                .build();
        infoApi = new InfoApi(esClient);
    }

    // 测试ping方法
    @Test
    public void test01() throws Exception {
        boolean ping = infoApi.ping();
        System.out.println(ping);
    }

    // 测试getMainInfo方法
    @Test
    public void test02() throws Exception {
        MainResponse mainInfo = infoApi.getMainInfo();
        String clusterName = mainInfo.getClusterName().value();
        String clusterUuid = mainInfo.getClusterUuid();
        String version = mainInfo.getVersion().toString();
        String nodeName = mainInfo.getNodeName();

        System.out.println(String.format("clusterName: %s, clusterUuid: %s, version: %s, nodeName: %s",
                clusterName, clusterUuid, version, nodeName));
    }

    @After
    public void cleanUp() throws Exception {
        esClient.close();
    }

}