package cn.xpleaf.commons.rest.es.api;

import cn.xpleaf.commons.rest.es.client.EsClient;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author xpleaf
 * @date 2019/1/6 5:45 PM
 *
 * 批量创建、更新或删除文档
 * Note：小数据量，可以直接使用WriterApi，因为简单，对于大数据量，使用性能更好的BulkWriterApi
 *
 * 关于bulkActions、bulkSize和flushInterval需要说明下。
 * 查看设置flushInterval时的注释说明：
 * Sets a flush interval flushing *any* bulk actions pending if the interval passes. Defaults to not set.
 * Note, both { #setBulkActions(int)} and { #setBulkSize(org.elasticsearch.common.unit.ByteSizeValue)}
 * can be set to <tt>-1</tt> with the flush interval set allowing for complete async processing of bulk actions.
 * 当设置了flushInterval时，相当于会定时刷新bulk的request，其是根据时间来进行刷新的。
 * bulkActions、bulkSize和flushInterval分别从数量、大小、时间三个不同的维度来定义了一个bulk请求被触发时需要达到的条件，
 * 只需要满足一个，即可以触发bulk的flush操作，当然，bulk api本身也提供了可手动进行操作的flush方法。
 */
public class BulkWriterApi {

    private static final Logger LOG = LoggerFactory.getLogger(BulkWriterApi.class);

    // 必选的设置
    private EsClient esClient;
    private String indexName;
    private String typeName;

    // 不可选的设置,bulkProcessor为内部使用
    private BulkProcessor bulkProcessor;

    protected BulkWriterApi(EsClient esClient, String indexName, String typeName, BulkProcessor bulkProcessor) {
        this.esClient = esClient;
        this.indexName = indexName;
        this.typeName = typeName;
        this.bulkProcessor = bulkProcessor;
    }

    // 对外创建BulkWriterApi的接口
    public static class Builder {
        // 设置请求操作的数据超过多少次触发批量提交操作，es客户端本身默认为1000
        private static final int DEFAULT_BULK_ACTIONS = 1000;
        // 设置批处理请求达到多少M触发批量提交动作，es客户端本身默认为5MB
        private static final int DEFAULT_BULK_SIZE = 5;
        // 自动刷新请求时间，es客户端本身默认没有设置，这里默认同样不设置
        private static final Integer DEFAULT_FLUSH_INTERVAL = null;
        // bulkRequest的默认超时时间，1分钟，现在统一使用默认，不开放
        private static final int DEFAULT_BULK_REQUEST_TIMEOUT = 1;
        // 并发执行的request个数，5个，不开放
        private static final int DEFAULT_CONCURRENT_REQUESTS = 5;

        // 必选的设置
        private EsClient esClient;
        private String indexName;
        private String typeName;

        // 可选的设置，不设置会使用默认值
        private int bulkActions = DEFAULT_BULK_ACTIONS;
        private int bulkSize = DEFAULT_BULK_SIZE;
        private Integer flushInterval = DEFAULT_FLUSH_INTERVAL;

        // 不可设置的选项
        private BulkProcessor bulkProcessor;

        public Builder(EsClient esClient, String indexName, String typeName) {
            this.esClient = esClient;
            this.indexName = indexName;
            this.typeName = typeName;
        }

        public Builder setBulkActions(int bulkActions) {
            if(bulkActions >= 0) {
                this.bulkActions = bulkActions;
            }
            return this;
        }

        public Builder setBulkSize(int bulkSize) {
            if(bulkSize >= 0) {
                this.bulkSize = bulkSize;
            }
            return this;
        }

        public Builder setFlushInterval(Integer flushInterval) {
            if(flushInterval >= 0) {
                this.flushInterval = flushInterval;
            }
            return this;
        }

        public BulkWriterApi build() {
            bulkProcessor = initBulkProcessor();
            return new BulkWriterApi(esClient, indexName, typeName, bulkProcessor);
        }

        // 构建BulkProcessor，同时设置相关参数
        private BulkProcessor initBulkProcessor() {
            // 构建ThreadPool
            Settings settings = Settings.builder().build();
            ThreadPool threadPool = new ThreadPool(settings);
            // 构建listener
            BulkProcessor.Listener listener = initListener();
            // 构建BulkProcessor，同时设置相关参数
            BulkProcessor.Builder builder = new BulkProcessor.Builder(esClient.client::bulkAsync, listener, threadPool);
            builder.setBulkActions(this.bulkActions)
                    .setBulkSize(new ByteSizeValue(this.bulkSize, ByteSizeUnit.MB))
                    .setConcurrentRequests(DEFAULT_CONCURRENT_REQUESTS)                      // 设置并发处理线程个数
                    .setBackoffPolicy(BackoffPolicy
                            .exponentialBackoff(TimeValue.timeValueMillis(100),
                                    3))   ;                   // 设置回滚策略，等待时间为100ms，retry次数为3次
            if(this.flushInterval != null) {
                builder.setFlushInterval(TimeValue.timeValueSeconds(flushInterval));
            }
            return builder.build();
        }

        /**
         * 构建BulkProcessor.Listener
         * 注意每一个批次的操作都会执行Listener中的方法
         * 比如设置了BulkActions为2，那么每两个请求，都会执行Listener中的方法
         * 参考：
         * https://github.com/xpleaf/rest-esApi-demo
         * /blob/dev/src/main/java/cn/xpleaf/es/restEs/_1singleDocAPIs/_5BulkAPI.java
         */
        private BulkProcessor.Listener initListener() {
            BulkProcessor.Listener listener = new BulkProcessor.Listener() {
                // 批量操作前
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                    LOG.info("---批量操作前---");
                    int numberOfActions = request.numberOfActions();
                    LOG.info(String.format("numberOfActions: %s", numberOfActions));
                    // 设置超时时间为1分钟
                    request.timeout(TimeValue.timeValueMinutes(DEFAULT_BULK_REQUEST_TIMEOUT));
                    LOG.info("---批量操作前---");
                }

                // 批量操作后
                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    LOG.info("---批量操作后---");
                    int status = response.status().getStatus();
                    LOG.info(String.format("status: %s", status));
                    /**
                     * 下面的操作不是必需的，在这里只是告诉读者，批量操作时，可能会出现某几请求失败的情况
                     * 因此这里是告诉读者一种方式，即如何找到失败的请求，之后可以再根据自己的策略进行重试
                     * 这样的失败处理策略只是为打log，因为在创建bulkProcessor会设置其内部的重试策略
                     */
                    // 同时可以获取到请求
                    List<DocWriteRequest> requests = request.requests();
                    // 并获取到对应的response
                    BulkItemResponse[] bulkItemResponses = response.getItems();
                    // 可以找到失败的请求
                    int successCount = 0;   // 统计成功的请求数
                    if(response.hasFailures()) {    // 说明至少有一个请求失败了
                        try {
                            // 遍历找到失败的请求，和与之对应的request，同时统计成功的请求数
                            for (int i = 0; i < bulkItemResponses.length; i++) {
                                BulkItemResponse bulkItemResponse = bulkItemResponses[i];
                                if (bulkItemResponse.isFailed()) {
                                    // 拿到失败的请求，其顺序与bulkItemResponses数组是一一对应的
                                    DocWriteRequest docWriteRequest = requests.get(i);
                                    // 判断各个请求所属的请求类型
                                    if (docWriteRequest instanceof IndexRequest) {
                                        IndexRequest indexRequest = (IndexRequest) docWriteRequest;
                                        LOG.error("创建请求失败了！" + indexRequest.toString());
                                        // TODO 创建请求失败处理策略
                                    } else if (docWriteRequest instanceof UpdateRequest) {
                                        UpdateRequest updateRequest = (UpdateRequest) docWriteRequest;
                                        LOG.error("更新请求失败了！" + updateRequest.toString());
                                        // TODO 更新请求失败处理策略
                                    } else if (docWriteRequest instanceof DeleteRequest) {
                                        DeleteRequest deleteRequest = (DeleteRequest) docWriteRequest;
                                        LOG.error("删除请求失败了！" + deleteRequest.toString());
                                        // 可以根据自己的重试策略重新添加到bulkProcessor中
                                        // bulkProcessor.add(deleteRequest);
                                        // TODO 删除请求失败处理策略
                                    }
                                    continue;
                                }
                                successCount++;
                            }
                        } catch (Exception e) {
                            LOG.error("统计请求失败操作时出现异常，信息为：" + e.getMessage());
                        }
                    } else {
                        successCount = bulkItemResponses.length;
                    }
                    LOG.info(String.format("成功请求数量：%s，消耗时间：%s ms", successCount, response.getTook().getMillis()));
                    LOG.info("---批量操作后---");
                }

                // 批量操作出现异常时
                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    LOG.error("---批量操作出现异常了---");
                    LOG.error("异常信息为：" + failure.getMessage());
                    LOG.error("---批量操作出现异常了---");
                }
            };
            return listener;
        }
    }


}
