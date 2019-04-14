package com.github.xpleaf.rest.es.action;

import io.searchbox.action.GenericResultAbstractAction;

/**
 * @author xpleaf
 * @date 2019/1/1 12:47 PM
 *
 * 自定义的action，通过别名获取其指向的索引名称（可以参考OpenIndex的定义）
 * Note:
 * 1.一个别名可以对应多个索引
 * 2.一个索引可以有多个别名
 */
public class GetAliasSpecificNames extends GenericResultAbstractAction {

    private String alias;

    // 使用protected，只允许该包下的类直接new，所以调用者需要使用下面提供的builder来创建其实例
    protected GetAliasSpecificNames(GetAliasSpecificNames.Builder builder, String alias) {
        super(builder);
        this.alias = alias;
        setURI(buildURI());
    }

    @Override
    protected String buildURI() {
        return super.buildURI() + "/_alias/" + alias;
    }

    @Override
    public String getRestMethodName() {
        return "GET";
    }

    public static class Builder extends GenericResultAbstractAction.Builder<GetAliasSpecificNames, Builder> {
        private String alias = "*"; // 不使用下面的alias方法设置别名时，默认为 _alias/* 会获取所有 indexName到alias 的映射

        public Builder alias(String alias) {
            this.alias = alias;
            return this;
        }

        @Override
        public GetAliasSpecificNames build() {
            return new GetAliasSpecificNames(this, alias);
        }
    }
}
