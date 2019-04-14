package com.github.xpleaf.rest.es.filter;

import com.github.xpleaf.rest.es.util.JsonPathUtil;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author xpleaf
 * @date 2019/1/5 9:54 PM
 *
 * 内置的拦截器，根据开发经验不断丰富
 */
public class BuildInFilter extends AbstractQueryDSLFilter {

    private static final Logger LOG = LoggerFactory.getLogger(BuildInFilter.class);

    @Override
    protected String handleForEsV17(String sourceQueryDSL) {
        return sourceQueryDSL;
    }

    @Override
    protected String handleForEsV23(String sourceQueryDSL) {
        try {
            sourceQueryDSL = functionScoreHandler(sourceQueryDSL);
        } catch (Exception e) {
            LOG.error("拦截处理DSL：{} 失败，原因为：{}", sourceQueryDSL, e.getMessage());
        }
        return sourceQueryDSL;
    }

    @Override
    protected String handleForEsV56(String sourceQueryDSL) {
        return sourceQueryDSL;
    }

    // es5.6构建的function Score DSL中，es2.3是不支持的，需要将query修改为filter
    private String functionScoreHandler(String sourceQueryDSL) {
        // 先读取query.function_score.query的内容
        String query = JsonPathUtil.read(sourceQueryDSL, "$.query.function_score.query");
        if(query != null) { // 确实是有这样的查询时才处理
            // 再删除query.function_score.query
            sourceQueryDSL = JsonPathUtil.delete(sourceQueryDSL, "$.query.function_score.query");
            // 再添加query.function_score.filter
            sourceQueryDSL = JsonPathUtil.put(sourceQueryDSL, "$.query.function_score", "filter", new Gson().fromJson(query, Map.class));
        }

        return sourceQueryDSL;
    }
}
