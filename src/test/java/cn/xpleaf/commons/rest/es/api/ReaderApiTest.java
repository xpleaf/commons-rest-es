package cn.xpleaf.commons.rest.es.api;

import cn.xpleaf.commons.rest.es.client.EsClient;
import cn.xpleaf.commons.rest.es.entity.EsReaderResult;
import cn.xpleaf.commons.rest.es.entity.EsSort;
import cn.xpleaf.commons.rest.es.enums.Sort;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author xpleaf
 * @date 2019/1/3 11:30 PM
 */
public class ReaderApiTest {

    EsClient esClient;
    ReaderApi readerApi;

    @Before
    public void init() throws Exception {
        esClient = new EsClient.Builder().setEsHosts("localhost:9200").build();
    }

    // 测试search方法
    @Test
    public void test01() throws Exception {
        readerApi = new ReaderApi(esClient)
                .setIndexName("spnews")
                .setTypeName("news");
        EsSort esSort = new EsSort.Builder().addSort("reply", Sort.DESC).build();
        EsReaderResult esReaderResult = readerApi.search(null,
                null,
                QueryBuilders.matchAllQuery(),
                null,
                esSort);
        System.out.println(esReaderResult);
    }

    @After
    public void cleanUp() throws Exception {
        esClient.close();
    }

}