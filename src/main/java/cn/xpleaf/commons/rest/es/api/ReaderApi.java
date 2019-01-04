package cn.xpleaf.commons.rest.es.api;

import cn.xpleaf.commons.rest.es.client.EsClient;
import cn.xpleaf.commons.rest.es.entity.EsDoc;
import cn.xpleaf.commons.rest.es.entity.EsReaderResult;
import cn.xpleaf.commons.rest.es.entity.EsSort;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author xpleaf
 * @date 2019/1/2 2:15 PM
 *
 * 文档搜索、聚合操作
 */
public class ReaderApi {

    private static Logger LOG = LoggerFactory.getLogger(ReaderApi.class);

    private static final long DEFAULT_TIMEOUT_MILLS = 120 * 1000;
    private static final long DEFAULT_SCROLL_TIME_WINDOW_MILLS = 60 * 1000;    // 默认的scroll时间窗口为60s

    private EsClient esClient;
    private String indexName;                               // 索引名称，不指定则查询所有索引
    private String typeName;                                // 类型名称，不指定则查询所有类型
    private long timeoutMills = DEFAULT_TIMEOUT_MILLS;       // Query DSL查询级别的超时时间，默认120秒

    public ReaderApi(EsClient esClient) {
        this.esClient = esClient;
    }

    public ReaderApi setIndexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    public ReaderApi setTypeName(String typeName) {
        this.typeName = typeName;
        return this;
    }

    public ReaderApi setTimeoutMills(long timeoutMills) {
        this.timeoutMills = timeoutMills;
        return this;
    }

    /**
     * 查询数据
     * @param from  从第几条数据开始，可以设置为null
     * @param size  数据条数，可以设置为null
     * @param queryBuilder      查询条件，如果为null则匹配全部
     * @param includeSource     选择的列，如果为null则默认选择全部
     * @param esSort            排序规则，如果为null则按照默认排序规则进行排序
     */
    public EsReaderResult search(Integer from,
                                 Integer size,
                                 QueryBuilder queryBuilder,
                                 String[] includeSource,
                                 EsSort esSort) throws Exception {
        // 设置查询条件
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if(queryBuilder != null) {
            searchSourceBuilder.query(queryBuilder);
        }
        if(from != null) {
            searchSourceBuilder.from(from);
        }
        if(size != null) {
            searchSourceBuilder.size(size);
        }
        if(includeSource != null) {
            searchSourceBuilder.fetchSource(includeSource, Strings.EMPTY_ARRAY);
        }
        if(esSort != null) {
            for(FieldSortBuilder fieldSortBuilder : esSort.sortOrderList) {
                searchSourceBuilder.sort(fieldSortBuilder);
            }
        }
        // 设置超时时间
        if(timeoutMills != 0) {
            searchSourceBuilder.timeout(TimeValue.timeValueMillis(timeoutMills));
        } else {
            searchSourceBuilder.timeout(TimeValue.timeValueMillis(DEFAULT_TIMEOUT_MILLS));
        }

        return search(searchSourceBuilder);
    }

    /**
     * 查询数据
     * @param searchSourceBuilder 查询条件
     */
    public EsReaderResult search(SearchSourceBuilder searchSourceBuilder) throws Exception {
        // 构建请求
        SearchRequest searchRequest = new SearchRequest();
        // 设置indexName和typeName
        if(indexName != null) {
            searchRequest.indices(indexName);
        }
        if(typeName != null) {
            searchRequest.types(typeName);
        }
        // 设置searchSourceBuilder
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = esClient.client.search(searchRequest);

        // 处理查询的结果到esReaderResult
        EsReaderResult esReaderResult = new EsReaderResult();
        List<EsDoc> esDocList = EsDoc.genEsDocList(searchResponse.getHits());
        long totalHits = searchResponse.getHits().getTotalHits();
        long took = searchResponse.getTookInMillis();
        double successRate = searchResponse.getSuccessfulShards() / (double) searchResponse.getTotalShards();
        esReaderResult.setEsDocList(esDocList).setTotalHits(totalHits).setTook(took).setSuccessRate(successRate);

        return esReaderResult;
    }

    /**
     * 初次scroll查询数据
     * @param scrollSize        每次获取的数据条数
     * @param queryBuilder      查询条件，如果为null则匹配全部
     * @param includeSource     选择的列，如果为null则默认选择全部
     * @param esSort            排序规则，如果为null则按照默认排序规则进行排序
     * @param timeWindowMills   时间窗口，如果为null则按照默认时间窗口60s
     */
    public EsReaderResult scroll(Integer scrollSize,
                                 QueryBuilder queryBuilder,
                                 String[] includeSource,
                                 EsSort esSort,
                                 Long timeWindowMills) throws Exception {
        // 构建searchSourceBuilder
        SearchSourceBuilder searchSourceBuilder = initSearchSourceBuilder(null, scrollSize, queryBuilder, includeSource, esSort);
        // 构建searchRequest
        SearchRequest searchRequest = initSearchRequest();
        // 设置searchSourceBuilder
        searchRequest.source(searchSourceBuilder);
        // 设置scroll时间窗口
        if(timeWindowMills != null && timeWindowMills > 0) {
            searchRequest.scroll(TimeValue.timeValueMillis(timeWindowMills));
        } else {
            searchRequest.scroll(TimeValue.timeValueMillis(DEFAULT_SCROLL_TIME_WINDOW_MILLS));
        }

        // 处理查询结果
        SearchResponse searchResponse = esClient.client.search(searchRequest);
        EsReaderResult esReaderResult = handleSearchResponse(searchResponse);

        // 封装scrollId
        esReaderResult.setScrollId(searchResponse.getScrollId());

        return esReaderResult;
    }

    /**
     * 获取scrollId后的scroll查询数据
     * @param scrollId  上一次scroll查询时返回的scrollId
     */
    public EsReaderResult scroll(String scrollId, Long timeWindowMills) throws Exception {
        // 需要通过SearchScrollRequest来构建查询对象
        SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
        if(timeWindowMills > 0) {
            scrollRequest.scroll(TimeValue.timeValueMillis(timeWindowMills));
        } else {
            scrollRequest.scroll(TimeValue.timeValueMillis(DEFAULT_SCROLL_TIME_WINDOW_MILLS));
        }
        // 处理查询结果
        SearchResponse searchScrollResponse = esClient.client.searchScroll(scrollRequest);
        EsReaderResult esReaderResult = handleSearchResponse(searchScrollResponse);

        // 封装scrollId，也可以按照原来的scrollId，但是这里确保是最新的
        esReaderResult.setScrollId(searchScrollResponse.getScrollId());

        return esReaderResult;
    }

    /**
     * 获取scrollId后的scroll查询数据
     * @param scrollId  上一次scroll查询时返回的scrollId
     */
    public EsReaderResult scroll(String scrollId) throws Exception {
        return scroll(scrollId, DEFAULT_SCROLL_TIME_WINDOW_MILLS);
    }

    /**
     * 初始化SearchSourceBuilder对象
     * @param from              从第几条数据开始，可以设置为null，scroll时，其也为null
     * @param size              数据条数，可以设置为null
     * @param queryBuilder      查询条件，如果为null则匹配全部
     * @param includeSource     选择的列，如果为null则默认选择全部
     * @param esSort            排序规则，如果为null则按照默认排序规则进行排序
     */
    private SearchSourceBuilder initSearchSourceBuilder(Integer from,
                                                        Integer size,
                                                        QueryBuilder queryBuilder,
                                                        String[] includeSource,
                                                        EsSort esSort) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        if(queryBuilder != null) {
            searchSourceBuilder.query(queryBuilder);
        }
        if(from != null) {
            searchSourceBuilder.from(from);
        }
        if(size != null) {
            searchSourceBuilder.size(size);
        }
        if(includeSource != null) {
            searchSourceBuilder.fetchSource(includeSource, Strings.EMPTY_ARRAY);
        }
        if(esSort != null) {
            for(FieldSortBuilder fieldSortBuilder : esSort.sortOrderList) {
                searchSourceBuilder.sort(fieldSortBuilder);
            }
        }
        // 设置超时时间
        if(timeoutMills > 0) {
            searchSourceBuilder.timeout(TimeValue.timeValueMillis(timeoutMills));
        } else {
            searchSourceBuilder.timeout(TimeValue.timeValueMillis(DEFAULT_TIMEOUT_MILLS));
        }
        return searchSourceBuilder;
    }

    /**
     * 初始化SearchRequest对象
     */
    private SearchRequest initSearchRequest() {
        // 构建请求
        SearchRequest searchRequest = new SearchRequest();
        // 设置indexName和typeName
        if(indexName != null) {
            searchRequest.indices(indexName);
        }
        if(typeName != null) {
            searchRequest.types(typeName);
        }
        return searchRequest;
    }

    // 关闭客户端
    public void close() {
        if(esClient != null) {
            esClient.close();
        }
    }

    /**
     * 将查询结果封装为EsReaderResult对象
     * @param searchResponse    查询结果
     */
    private EsReaderResult handleSearchResponse(SearchResponse searchResponse) {
        EsReaderResult esReaderResult = new EsReaderResult();
        List<EsDoc> esDocList = EsDoc.genEsDocList(searchResponse.getHits());
        long totalHits = searchResponse.getHits().getTotalHits();
        long took = searchResponse.getTookInMillis();
        double successRate = searchResponse.getSuccessfulShards() / (double) searchResponse.getTotalShards();
        esReaderResult.setEsDocList(esDocList).setTotalHits(totalHits).setTook(took).setSuccessRate(successRate);

        return esReaderResult;
    }

}
