package com.github.xpleaf.rest.es.filter;

import com.github.xpleaf.rest.es.enums.EsVersion;

/**
 * @author xpleaf
 * @date 2019/1/4 6:41 PM
 *
 * 拦截器抽象类，继承该抽象类，实现其对应版本的方法即可自定义拦截规则
 * Note：
 * 实现时，如果对应版本添加了自定义拦截器，但没有拦截时，不要返回null，直接返回sourceQueryDSL
 */
public abstract class AbstractQueryDSLFilter {

    // 拦截处理入口
    public String handle(EsVersion esVersion, String sourceQueryDSL) {
        switch (esVersion) {
            case V17:
                return handleForEsV17(sourceQueryDSL);
            case V23:
                return handleForEsV23(sourceQueryDSL);
            case V56:
            case DEFAULT:
            default:
                return handleForEsV56(sourceQueryDSL);
        }
    }

    // es1.7
    abstract protected String handleForEsV17(String sourceQueryDSL);

    // es2.3
    abstract protected String handleForEsV23(String sourceQueryDSL);

    // es5.6，如果是5.6的，其实这里没有意义，因为依赖的客户端就是es 5.6的，不过主要是考虑是用户可能会针对query DSL做一些处理
    abstract protected String handleForEsV56(String sourceQueryDSL);

}
