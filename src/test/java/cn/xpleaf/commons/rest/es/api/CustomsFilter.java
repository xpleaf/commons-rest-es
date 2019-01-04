package cn.xpleaf.commons.rest.es.api;

import cn.xpleaf.commons.rest.es.filter.AbstractQueryDSLFilter;

/**
 * @author xpleaf
 * @date 2019/1/4 7:38 PM
 */
public class CustomsFilter extends AbstractQueryDSLFilter {

    @Override
    protected String handleForEsV17(String sourceQueryDSL) {
        return null;
    }

    @Override
    protected String handleForEsV23(String sourceQueryDSL) {
        return null;
    }

    @Override
    protected String handleForEsV56(String sourceQueryDSL) {
        System.out.println("sourceQueryDSL: \n" + sourceQueryDSL);
        return sourceQueryDSL;
    }
}
