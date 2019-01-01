package cn.xpleaf.commons.rest.es.api;

import cn.xpleaf.commons.rest.es.action.GetAliasSpecificNames;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.JestResult;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.*;
import io.searchbox.indices.mapping.GetMapping;
import io.searchbox.indices.mapping.PutMapping;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

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
     * factory和client初始化
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

    /**
     * 判断索引是否存在
     * @param indexName 索引名称
     * @return 1.索引存在返回true 2.索引不存在返回false
     */
    public boolean indexExists(String indexName) throws IOException {
        IndicesExists indicesExists = new IndicesExists
                .Builder(indexName).build();
        JestResult jestResult = client.execute(indicesExists);
        return jestResult.isSucceeded();
    }

    /**
     * 创建索引
     * @param indexName 索引名称
     * @param settings  索引设置
     * @return 1.创建成功返回true 2.创建失败返回false
     */
    public boolean createIndex(String indexName, Map<Object, Object> settings) throws IOException {
        if (indexExists(indexName)) {
            // 如果索引存在，直接返回false
            return false;
        }
        // 需要先创建一个索引，才能设置mapping
        JestResult jestResult = client.execute(new CreateIndex.Builder(indexName)
                .settings(Settings.builder().build().getAsMap()).build());
        return jestResult.isSucceeded();
    }

    /**
     * 创建类型，要求索引必须存在
     * @param indexName     索引名称
     * @param indexType     类型名称
     * @param properties    类型对应的properties，json格式，可包含类型名称或不包括
     * @return 1.创建成功返回true（type存在时也会返回true，此时相当于是更新操作） 2.创建失败返回false
     */
    public boolean createType(String indexName, String indexType, String properties) throws IOException {
        // 创建PutMapping对象
        PutMapping putMapping = new PutMapping.Builder(indexName, indexType, properties).build();
        JestResult jestResult = client.execute(putMapping);
        return jestResult.isSucceeded();
    }

    /**
     * 创建类型，同时传入settings，如果索引不存在会自动创建索引
     * 1.如果索引不存在，则同时创建索引和type
     * 2.如果索引存在，则只创建type
     * @return 1.创建成功返回true 2.创建失败返回false
     * @param indexName     索引名称
     * @param indexType     类型名称
     * @param settings      索引设置，只有当索引不存在时，该字段才有效
     * @param properties    类型对应的properties，json格式，可包含类型名称或不包括
     */
    public boolean createType(String indexName, String indexType, Map<Object, Object> settings, String properties) throws IOException {
        // 先创建索引
        createIndex(indexName, settings);
        // 如果上面操作后，索引还是不存在，返回false
        if(!indexExists(indexName))
            return false;
        // 索引存在后才能添加type，否则会失败
        return createType(indexName, indexType, properties);
    }

    /**
     * 打开一个索引
     * @param indexName 索引名称
     */
    public boolean openIndex(String indexName) {
        OpenIndex openIndex = new OpenIndex.Builder(indexName).build();
        try {
            JestResult jestResult = client.execute(openIndex);
            return jestResult.isSucceeded();
        } catch (IOException e) {
            LOG.warn("打开索引失败，原因为：{}", e.getMessage());
        }
        return false;
    }

    /**
     * 关闭一个索引
     * @param indexName 索引名称
     */
    public boolean closeIndex(String indexName) {
        CloseIndex closeIndex = new CloseIndex.Builder(indexName).build();
        try {
            JestResult jestResult = client.execute(closeIndex);
            return jestResult.isSucceeded();
        } catch (IOException e) {
            LOG.warn("关闭索引失败，原因为：{}", e.getMessage());
        }
        return false;
    }

    /**
     * 删除一个索引
     * @param indexName 索引名称
     */
    public boolean deleteIndex(String indexName) {
        DeleteIndex deleteIndex = new DeleteIndex.Builder(indexName).build();
        try {
            JestResult jestResult = client.execute(deleteIndex);
            return jestResult.isSucceeded();
        } catch (IOException e) {
            LOG.warn("删除索引{}失败，原因为：{}", indexName, e.getMessage());
        }
        return false;
    }

    /**
     * 通过别名获取指定的索引名称
     * @param alias 索引别名
     * @return 索引别名指向的索引名称，一个别名可以指向多个索引，所以返回Set
     */
    public Set<String> getIndexByAlias(String alias) {
        Set<String> indexSet = null;
        GetAliasSpecificNames getAliasSpecificNames = new GetAliasSpecificNames.Builder().alias(alias).build();
        try {
            JestResult jestResult = client.execute(getAliasSpecificNames);
            if (jestResult.isSucceeded()) {
                indexSet = jestResult.getJsonObject().keySet();
            }
        } catch (IOException e) {
            LOG.warn("通过别名获取索引失败，原因为：{}", e.getMessage());
        }
        return indexSet;
    }

    /**
     * 获取索引下某个type的mapping信息（schema信息）
     * @param indexName 索引名称，可以为别名，因为通过别名也可以获取到mapping信息，
     *                  但如果别名对应有多个索引，只返回第一个对应索引的mapping
     * @param indexType 索引类型
     * @return 返回一个包含type字段信息的map，k为字段名称，v为其类型信息
     */
    @SuppressWarnings("unchecked")
    public Map<String, String> getMapping(String indexName, String indexType) throws IOException {
        GetMapping getMapping = new GetMapping.Builder().build();
        JestResult jestResult = client.execute(getMapping);
        // 别名处理，indexName有可能是别名，先将其作为别名来获取对应的索引名称
        Set<String> indexSet = getIndexByAlias(indexName);
        if(indexSet != null && !indexSet.isEmpty()) {   // 说明indexName是一个别名
            if(indexSet.size() > 1) {
                LOG.warn("{} 索引别名对应有多个索引，只取其中一个索引", indexName);
            }
            indexName = indexSet.toArray()[0].toString();
        }
        if(jestResult.isSucceeded()) {
            // 拿到所有索引的mapping
            JsonObject indicesJsonObject = jestResult.getJsonObject();
            // 获取indexName/typeName的mapping
            try {
                JsonObject typeJsonObject = indicesJsonObject
                        .getAsJsonObject(indexName)
                        .getAsJsonObject("mappings")
                        .getAsJsonObject(indexType)
                        .getAsJsonObject("properties");
                // 将其转换为map对象
                String mappingJson = typeJsonObject.toString();
                return new Gson().fromJson(mappingJson, Map.class);
            } catch (Exception e) {
                LOG.warn("获取mapping信息失败，原因为：{}", e.getMessage());
            }
        }
        return null;
    }

    /**
     * 关闭客户端连接
     * JestClient不用手动去执行关闭也行，因为其继承了AutoCloseable
     * @throws IOException
     */
    public void close() throws IOException {
        client.close();
    }


}
