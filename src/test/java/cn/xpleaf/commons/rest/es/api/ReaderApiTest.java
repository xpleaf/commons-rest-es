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
        String[] includeSource = {"postdate", "reply", "source", "title"};
        EsSort esSort = new EsSort.Builder().addSort("reply", Sort.DESC).build();
        EsReaderResult esReaderResult = readerApi.search(
                null,
                null,
                QueryBuilders.matchAllQuery(),
                includeSource,
                esSort);
        System.out.println(esReaderResult);
    }

    // 测试scroll方法
    @Test
    public void test02() throws Exception {
        readerApi = new ReaderApi(esClient)
                .setIndexName("spnews")
                .setTypeName("news");
        String[] includeSource = {"postdate", "reply", "source", "title"};
        EsSort esSort = new EsSort.Builder().addSort("reply", Sort.DESC).build();
        int scrollSize = 10;
        // 第一次scroll查询
        EsReaderResult esReaderResult = readerApi.scroll(
                scrollSize,
                QueryBuilders.matchAllQuery(),
                includeSource,
                esSort,
                null);
        System.out.println(esReaderResult);
        // 通过scrollId获取后面的数据批次
        EsReaderResult esReaderResult1 = readerApi.scroll(esReaderResult.getScrollId());
        System.out.println(esReaderResult1);

        // 两次的scrollId是一样的
        System.out.println(esReaderResult.getScrollId().equals(esReaderResult1.getScrollId()));

        // Note：当数据已经scroll获取完之后，最后一次esReaderResult的esDocList大小为0，
        // 这个用户可以基于此进行来判断是否已经遍历完数据
    }

    @After
    public void cleanUp() throws Exception {
        esClient.close();
    }

}