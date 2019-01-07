# commons-rest-es

- [Overview](#Overview)
- [QuickStart](#QuickStart)
    - [IndexApi](#IndexApi)
    - [ReaderApi](#ReaderApi)
    - [WriterApi](#WriterApi)
    - [BulkWriterApi](#BulkWriterApi)
    - [InfoApi](#InfoApi)
    - [JsonPathUtil](#JsonPathUtil)
        - [JsonPathUtilTest](#JsonPathUtilTest)
        - [JsonPathTutorial](#JsonPathTutorial)
## Overview
Easy to use es rest api, the wrapper of elasticsearch-rest-high-level-client and Jest API, including the custom filter module to compatible with different version of es.

commons-rest-es是一个非常容易使用的es-api公共套件，它基于现阶段es官方十分推崇的elasticsearch-rest-high-level-client和Jest API封装了常用的es操作，目前提供了IndexApi、ReaderApi、WriterApi、BulkWriterApi和InfoApi：
- IndexApi：主要用来进行索引和类型的操作，包括索引创建、类型创建、mapping信息获取等；
- ReaderApi：主要用来搜索es中的数据，包括一般的搜索和聚合操作等；
- WriterApi：主要用来修改es中的数据，包括写入数据、更新数据和删除数据等；
- BulkWriterApi：WriterApi的加强版，其主要提供了批量操作的功能，以提高操作性能；
- InfoApi：主要用来获取es集群的的信息，包括连通性测试、主要信息获取等；

并且**commons-rest-es修改了elasticsearch-rest-high-level-client的部分源码，通过注入自定义拦截器来实现向下兼容不同es版本的目的**，使用它，将极大提高进行es开发的效率。
> 关于自定义拦截器的使用，可以参考下面的QuickStart ReaderApi部分。

commons-rest-es的使用非常简单，可以参考下面精心提供的测试案例教程。

## QuickStart

### IndexApi
IndexApi主要用来进行索引和类型的操作，包括索引创建、类型创建、mapping信息获取等。
```java
public class IndexApiTest {

    private IndexApi indexApi = null;

    @Before
    public void setUp() throws Exception {
        indexApi = new IndexApi("localhost:9200");
    }

    @Test
    public void indexExists() {
        System.out.println(indexApi.indexExists("my_index"));
    }

    @Test
    public void createIndex() {
        HashMap<Object, Object> settings = new HashMap<>();
        settings.put("number_of_shards", 3);
        settings.put("number_of_replicas", 1);
        System.out.println(indexApi.createIndex("my_index", settings));
    }

    @Test
    public void createType() {
        String mapping = "{\n" +
                "  \"properties\":{\n" +
                "    \"title\":{\n" +
                "      \"type\":\"keyword\"\n" +
                "    },\n" +
                "    \"content\":{\n" +
                "      \"type\":\"text\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        HashMap<Object, Object> settings = new HashMap<>();
        settings.put("number_of_shards", 3);
        settings.put("number_of_replicas", 1);
        System.out.println(indexApi.createType("my_index", "my_type", settings, mapping));
    }

    @Test
    public void createType1() {
        String mapping = "{\n" +
                "  \"properties\":{\n" +
                "    \"title\":{\n" +
                "      \"type\":\"keyword\"\n" +
                "    },\n" +
                "    \"content\":{\n" +
                "      \"type\":\"text\"\n" +
                "    }\n" +
                "  }\n" +
                "}";
        System.out.println(indexApi.createType("my_index", "my_type", mapping));
    }

    @Test
    public void openIndex() {
        System.out.println(indexApi.openIndex("my_index"));
    }

    @Test
    public void closeIndex() {
        System.out.println(indexApi.closeIndex("my_index"));
    }

    @Test
    public void deleteIndex() {
        System.out.println(indexApi.deleteIndex("my_index"));
    }

    @Test
    public void indexList() {
        System.out.println(indexApi.indexList());
    }

    @Test
    public void getIndexByAlias() {
        System.out.println(indexApi.getIndexByAlias("alias_test"));
    }

    @Test
    public void getMapping() {
        System.out.println(indexApi.getMapping("my_index", "my_type"));
    }

    @Test
    public void indexSize() {
        System.out.println(indexApi.indexSize("my_index"));
    }

    @Test
    public void indexSize1() {
        System.out.println(indexApi.indexSize("my_index", SizeUnit.BYTES));
    }

    @After
    public void tearDown() throws Exception {
        indexApi.close();
    }
}
```

### ReaderApi
ReaderApi主要用来搜索es中的数据，包括一般的搜索和聚合操作等。
```java
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
```
其用到的自定义拦截器如下：
```java
public class CustomsFilter extends AbstractQueryDSLFilter {

    @Override
    protected String handleForEsV17(String sourceQueryDSL) {
        return sourceQueryDSL;
    }

    @Override
    protected String handleForEsV23(String sourceQueryDSL) {
        return sourceQueryDSL;
    }

    @Override
    protected String handleForEsV56(String sourceQueryDSL) {
        System.out.println("sourceQueryDSL: \n" + sourceQueryDSL);
        return sourceQueryDSL;
    }
}
```

### WriterApi
WriterApi主要用来修改es中的数据，包括写入数据、更新数据和删除数据等。
```java
public class WriterApiTest {

    EsClient esClient;
    WriterApi writerApi;

    @Before
    public void init() throws Exception {
        esClient = new EsClient.Builder().setEsHosts("localhost:9200").build();
    }

    // 测试insertDoc方法，实时插入一条数据
    @Test
    public void test01() throws Exception {
        writerApi = new WriterApi(esClient, "bigdata", "stack");
        // 指定id
        Map<String, Object> dataMap = new HashMap<String, Object>(){{
            put("keyword", "elasticsearch");
            put("content", "do you like elasticsearch?");
        }};
        EsDoc esDoc = new EsDoc("1", dataMap);
        boolean isInsert = writerApi.insertDoc(esDoc);
        if(isInsert) {
            System.out.println("插入数据成功！插入的数据为：" + esDoc);
        }
        // 不指定id
        Map<String, Object> dataMap1 = new HashMap<String, Object>(){{
            put("keyword", "spark");
            put("content", "do you like spark?");
        }};
        EsDoc esDoc1 = new EsDoc(dataMap1);
        boolean isInsert1 = writerApi.insertDoc(esDoc1);
        if(isInsert1) {
            System.out.println("插入数据成功！插入的数据为：" + esDoc1);
        }
    }

    // 测试updateDoc方法，实时更新一条文档
    @Test
    public void test02() throws Exception {
        writerApi = new WriterApi(esClient, "bigdata", "stack");
        Map<String, Object> dataMap = new HashMap<String, Object>(){{
            put("content", "do you like es?");   // 修改
            put("tag", "es");                    // 新增字段
        }};
        EsDoc esDoc = new EsDoc("1", dataMap);
        boolean isUpdate = writerApi.updateDoc(esDoc);
        if(isUpdate) {
            System.out.println("更新文档成功！");
        }
    }

    // 测试updateDoc方法，upsert操作，更新或插入操作
    @Test
    public void test03() throws Exception {
        writerApi = new WriterApi(esClient, "bigdata", "stack");
        Map<String, Object> dataMap = new HashMap<String, Object>(){{
            put("keyword", "hadoop");
            put("content", "do you like hadoop?");
        }};
        EsDoc esDoc = new EsDoc("2", dataMap);
        boolean isUpdate = writerApi.updateDoc(esDoc, true);
        if(isUpdate) {
            System.out.println("更新或插入文档成功！");
        }
    }

    // 测试deleteDoc方法
    @Test
    public void test04() throws Exception {
        writerApi = new WriterApi(esClient, "bigdata", "stack");
        boolean isDeleted = writerApi.deleteDoc("2");
        if(isDeleted) {
            System.out.println("删除文档成功！");
        }
    }

    @After
    public void cleanUp() throws Exception {
        esClient.close();
    }

}
```

### BulkWriterApi
BulkWriterApi是WriterApi的加强版，其主要提供了批量操作的功能，以提高操作性能。
```java
public class BulkWriterApiTest {

    // 直接在main方法中操作，因为内部的threadPool在test case下无法创建
    public static void main(String[] args) throws Exception {
        // 初始化连接客户端
        EsClient esClient = new EsClient.Builder()
                .setEsHosts("localhost:9200")
                .setEsVersion(EsVersion.V56)
                .build();
        BulkWriterApi writerApi = new BulkWriterApi.Builder(esClient, "bulk_index", "bulkType")
                .setBulkActions(1000)
                .setBulkSize(5)
                .setFlushInterval(10)
                .build();
        // 构建写入的数据
        Map<String, Object> dataMap = new HashMap<>();
        dataMap.put("title", "Do you like elasticsearch and spark?");

        // 插入文档
        for (int i = 0; i < 20; i++) {
            writerApi.insertDoc(new EsDoc(i + "", dataMap));
        }
        writerApi.flush();

        // 等待一下，因为即便上面进行了flush操作，但是es内部仍然有可能还没有那么快更新，会导致下面的部分更新操作失败
        Thread.sleep(2000);

        // 更新文档
        dataMap.put("content", "Of cause I love es and spark, and you?");
        for (int i = 0; i < 10; i++) {
            writerApi.updateDoc(new EsDoc(i + "", dataMap));
        }

        Thread.sleep(2000);

        // 删除文档
        Random random = new Random();
        for (int i = 0; i < 5; i++) {
            writerApi.deleteDoc(random.nextInt(15) + "");
        }
        writerApi.flush();

        // 等待关闭
        writerApi.close();

    }

}
```

### InfoApi
InfoApi主要用来获取es集群的的信息，包括连通性测试、主要信息获取等。
```java
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
```

### JsonPathUtil
commons-rest-es还提供了JsonPathUtil，其是基于JsonPath做了简单的封装，通过使用它，就可以对不同版本的es queryDSL语句做拦截，完全实现自定义兼容不同版本es的目的。

下面提供了两个测试案例:
- JsonPathUtilTest：就是JsonPathUtil的使用案例，开发人员使用它就可以实现快速处理已知的queryDSL语句。
- JsonPathTutorial：官方JsonPath的快速入门教程，JsonPathUtil正是基于这些简单的测试案例来进行开发。
#### JsonPathUtilTest
```java
public class JsonPathUtilTest {

    String json = "{\n" +
            "  \"size\" : 0,\n" +
            "  \"timeout\" : \"20000ms\",\n" +
            "  \"query\" : {\n" +
            "    \"bool\" : {\n" +
            "      \"must\" : [\n" +
            "        {\n" +
            "          \"range\" : {\n" +
            "            \"2713_收入\" : {\n" +
            "              \"from\" : 12000,\n" +
            "              \"to\" : null,\n" +
            "              \"include_lower\" : false,\n" +
            "              \"include_upper\" : true,\n" +
            "              \"boost\" : 1.0\n" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      ],\n" +
            "      \"disable_coord\" : false,\n" +
            "      \"adjust_pure_negative\" : true,\n" +
            "      \"boost\" : 1.0\n" +
            "    }\n" +
            "  }\n" +
            "}";

    @Test
    public void read() {
        String result = JsonPathUtil.read(json, "$.query.bool.must[0].range");
        System.out.println(result);
    }

    @Test
    public void add() {
        Map<String, Object> mapNode = new HashMap<>();
        mapNode.put("match_all", new HashMap<>());
        String result = JsonPathUtil.add(json, "$.query.bool.must", mapNode);
        System.out.println(result);
    }

    @Test
    public void update() {
        String result = JsonPathUtil.update(json, "$.query..include_lower", true);
        System.out.println(result);
    }

    @Test
    public void delete() {
        String result = JsonPathUtil.delete(json, "$.query..include_lower", "$.query..include_upper", "$.query..boost");
        System.out.println(result);
    }

    @Test
    public void complexTest() {
        /**
         * 下面的查询是es 5.6的查询语法，es 1.7的大致相同，但是其function_score下面的query，
         * 不叫query，而是叫做filter，所以只需要把"query"替换成"filter"即可
         */
        json = "{\n" +
                "  \"size\": 100, \n" +
                "  \"query\": {\n" +
                "    \"function_score\": {\n" +
                "      \"query\": {\n" +
                "        \"bool\": {\n" +
                "          \"must\": [\n" +
                "            {\n" +
                "              \"range\": {\n" +
                "                \"publish_time\": {\n" +
                "                  \"from\": \"1541952000262\",\n" +
                "                  \"to\": \"1542023525262\"\n" +
                "                }\n" +
                "              }\n" +
                "            },\n" +
                "            {\n" +
                "              \"exists\":{\n" +
                "                \"field\":\"topics\"\n" +
                "              }\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      },\n" +
                "      \"functions\": [\n" +
                "        {\n" +
                "          \"random_score\": {\"seed\": \"1542023525262\"}\n" +
                "          \n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  },\n" +
                "  \"_source\": [ \"uid\", \"publish_time\", \"data_type\", \"content\", \"url\", \"pic_urls\", \"topics\" ]\n" +
                "}";
        // 先读取query.function_score.query的内容
        String query = JsonPathUtil.read(json, "$.query.function_score.query");
        // 再删除query.function_score.query
        json = JsonPathUtil.delete(this.json, "$.query.function_score.query");
        // 再添加query.function_score.filter
        json = JsonPathUtil.put(this.json, "$.query.function_score", "filter", new Gson().fromJson(query, Map.class));

        System.out.println(json);
    }
}
```

#### JsonPathTutorial
```java
public class JsonPathTutorial {

    String json = "{\n" +
            "  \"size\" : 0,\n" +
            "  \"timeout\" : \"20000ms\",\n" +
            "  \"query\" : {\n" +
            "    \"bool\" : {\n" +
            "      \"must\" : [\n" +
            "        {\n" +
            "          \"range\" : {\n" +
            "            \"2713_收入\" : {\n" +
            "              \"from\" : 12000,\n" +
            "              \"to\" : null,\n" +
            "              \"include_lower\" : false,\n" +
            "              \"include_upper\" : true,\n" +
            "              \"boost\" : 1.0\n" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      ],\n" +
            "      \"disable_coord\" : false,\n" +
            "      \"adjust_pure_negative\" : true,\n" +
            "      \"boost\" : 1.0\n" +
            "    }\n" +
            "  }\n" +
            "}";

    // 1.读取
    @Test
    public void test01() throws Exception {
        DocumentContext documentContext = JsonPath.parse(json);
        String path = "$.query..include_lower";
        Object obj = documentContext.read(path);
        System.out.println(obj);
    }

    // 2.添加 "match_all":{}
    @Test
    public void test02() throws Exception {
        DocumentContext documentContext = JsonPath.parse(json);
        Map<String, Object> mapNode = new HashMap<>();
        mapNode.put("match_all", new HashMap<>());
        String path = "$.query..must";
        documentContext = documentContext.add(path, mapNode);
        String json = documentContext.jsonString();
        System.out.println(json);
    }

    // 3.删除 disable_coord、adjust_pure_negative、boost、include_lower、include_upper
    @Test
    public void test03() throws Exception {
        DocumentContext documentContext = JsonPath.parse(json);
        documentContext = documentContext
                .delete("$.query..disable_coord")
                .delete("$.query..adjust_pure_negative")
                .delete("$.query..boost")
                .delete("$.query..include_lower")
                .delete("$.query..include_upper");
        String json = documentContext.jsonString();
        System.out.println(json);
    }

    // 4.更新range查询
    @Test
    public void test04() throws Exception {
        DocumentContext documentContext = JsonPath.parse(json);
        String newRange = "{\n" +
                "    \"2713_收入\":{\n" +
                "        \"from\":12000,\n" +
                "        \"to\":20000\n" +
                "    }\n" +
                "}";
        documentContext = documentContext.set("$.query..range", new Gson().fromJson(newRange, Map.class));
        String json = documentContext.jsonString();
        System.out.println(json);
    }

    // 5.修改range为term（修改之后实际上在es中没有这样term后面加from to的语法，这里只是作为一种演示）
    @Test
    public void test05() throws Exception {
        DocumentContext documentContext = JsonPath.parse(json);
        Object rangeObj = documentContext.read("$.query.bool.must[0].range");
        Map<Object, Object> termMap = new HashMap<>();
        termMap.put("term", rangeObj);
        // 先删除range，再设置term，term中的内容就是range原来的内容，这样就做到了只替换"range"为"term"的目的
        documentContext = documentContext
                .delete("$.query.bool.must[0]")
                .add("$.query.bool.must", termMap);
        String json = documentContext.jsonString();
        System.out.println(json);
    }


    public static void main(String[] args) throws IOException {
        String json = "{\n" +
                "  \"size\" : 0,\n" +
                "  \"timeout\" : \"20000ms\",\n" +
                "  \"query\" : {\n" +
                "    \"bool\" : {\n" +
                "      \"must\" : [\n" +
                "        {\n" +
                "          \"range\" : {\n" +
                "            \"2713_收入\" : {\n" +
                "              \"from\" : 12000,\n" +
                "              \"to\" : null,\n" +
                "              \"include_lower\" : false,\n" +
                "              \"include_upper\" : true,\n" +
                "              \"boost\" : 1.0\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ],\n" +
                "      \"disable_coord\" : false,\n" +
                "      \"adjust_pure_negative\" : true,\n" +
                "      \"boost\" : 1.0\n" +
                "    }\n" +
                "  }\n" +
                "}";

        DocumentContext documentContext = JsonPath.parse(json);
        // JsonPath p = JsonPath.compile("$.objs[0].obj");

        // 1.读取
        String path = "$.query..include_lower";
        Object obj = documentContext.read(path);
        System.out.println(obj);

        // 2.添加 "match_all":{}
        Map<String, Object> mapNode = new HashMap<>();
        mapNode.put("match_all", new HashMap<>());
        path = "$.query..must";
        DocumentContext documentContext1 = documentContext.add(path, mapNode);
        String json1 = documentContext1.jsonString();
        System.out.println(json1);

        // 3.删除 disable_coord、adjust_pure_negative、boost、include_lower、include_upper
        DocumentContext documentContext2 = documentContext
                .delete("$.query..disable_coord")
                .delete("$.query..adjust_pure_negative")
                .delete("$.query..boost")
                .delete("$.query..include_lower")
                .delete("$.query..include_upper");
        String json2 = documentContext2.jsonString();
        System.out.println(json2);

        // 4.更新range查询
        String newRange = "{\n" +
                "    \"2713_收入\":{\n" +
                "        \"from\":12000,\n" +
                "        \"to\":20000\n" +
                "    }\n" +
                "}";
        DocumentContext documentContext3 = documentContext.set("$.query..range", new Gson().fromJson(newRange, Map.class));
        String json3 = documentContext3.jsonString();
        System.out.println(json3);

        // 5.修改range为term（修改之后实际上在es中没有这样的语法，这里只是作为一种演示）
        Object rangeObj = documentContext.read("$.query.bool.must[0].range");
        Map<Object, Object> termMap = new HashMap<>();
        termMap.put("term", rangeObj);
        // 先删除range，再设置term，term中的内容就是range原来的内容，这样就做到了只替换"range"为"term"的目的
        DocumentContext documentContext4 = documentContext
                .delete("$.query.bool.must[0]")
                .add("$.query.bool.must", termMap);
        String json4 = documentContext4.jsonString();
        System.out.println(json4);

    }

}
```