package com.github.xpleaf.rest.es.api;

import com.github.xpleaf.rest.es.client.EsClient;
import com.github.xpleaf.rest.es.entity.EsReaderResult;
import com.github.xpleaf.rest.es.entity.EsSort;
import com.github.xpleaf.rest.es.enums.EsVersion;
import com.github.xpleaf.rest.es.enums.Sort;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

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

    // 测试aggSearch方法
    @Test
    public void test03() throws Exception {
        readerApi = new ReaderApi(esClient)
                .setIndexName("spnews")
                .setTypeName("news");
        // 构建聚合条件
        TermsAggregationBuilder groupBySource = AggregationBuilders.terms("group_by_source").field("source").size(10).minDocCount(1);
        TermsAggregationBuilder groupByReply = AggregationBuilders.terms("group_by_reply").field("reply").size(10).minDocCount(1);
        // 获取查询结果
        Map<String, Aggregation> aggregationMap = readerApi.aggSearch(QueryBuilders.matchAllQuery(), groupBySource, groupByReply);

        // 遍历聚合结果
        for(String key : aggregationMap.keySet()) {
            System.out.println("-------------------------------------------->" + key);
            Aggregation aggregation = aggregationMap.get(key);
            // 转换为Terms
            Terms termsAggregation = (Terms) aggregation;
            if(termsAggregation.getBuckets().size() > 0) {
                for(Terms.Bucket bucket : termsAggregation.getBuckets()) {
                    Object bucketKey = bucket.getKey();
                    long docCount = bucket.getDocCount();
                    System.out.println(String.format("bucket: %s, docCount: %s", bucketKey, docCount));
                }
            }
        }
        System.out.println();
    }

    // 测试自定义拦截器
    @Test
    public void test04() throws Exception {
        esClient = new EsClient.Builder()
                .setEsHosts("localhost:9200")
                .setEsVersion(EsVersion.V56)
                .addFilter(new CustomsFilter())
                .build();
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

    @After
    public void cleanUp() throws Exception {
        esClient.close();
    }

}