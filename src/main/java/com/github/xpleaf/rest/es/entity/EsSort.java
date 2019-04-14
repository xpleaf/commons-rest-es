package com.github.xpleaf.rest.es.entity;

import com.github.xpleaf.rest.es.enums.Sort;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xpleaf
 * @date 2019/1/3 5:32 PM
 *
 * 排序实体
 */
public class EsSort {

    // 包含排序规则的列表，现在的排序都是基于字段内容来进行排序
    public List<FieldSortBuilder> sortOrderList;

    protected EsSort(List<FieldSortBuilder> sortOrderList) {
        this.sortOrderList = sortOrderList;
    }

    public static class Builder {

        private List<FieldSortBuilder> sortOrderList = new ArrayList<>();

        public Builder addSort(String fieldName, Sort sort) {
            if("asc".equals(sort.getOrder())) {
                this.sortOrderList.add(new FieldSortBuilder(fieldName).order(SortOrder.ASC));
            } else if("desc".equals(sort.getOrder())) {
                this.sortOrderList.add(new FieldSortBuilder(fieldName).order(SortOrder.DESC));
            }
            return this;
        }

        public EsSort build() {
            return new EsSort(this.sortOrderList);
        }

    }

    /*public static void main(String[] args) {
        EsSort hello = new Builder().addSort("hello", Sort.ASC).build();
    }*/

}
