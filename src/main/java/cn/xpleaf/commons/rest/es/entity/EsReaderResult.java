package cn.xpleaf.commons.rest.es.entity;

import java.util.List;

/**
 * @author xpleaf
 * @date 2019/1/3 11:10 PM
 */
public class EsReaderResult {

    private List<EsDoc> esDocList;  // 当前查询的document数据
    private long totalHits;         // 查询总的数量，totalHits >= esDocList.size，因为有可能分页查询
    private long took;              // 当前查询所消耗的时间，ms
    private double successRate;     // 当前查询的成功率，successfulShards / totalShards

    private String scrollId;        // scroll查询时才会有

    public List<EsDoc> getEsDocList() {
        return esDocList;
    }

    public EsReaderResult setEsDocList(List<EsDoc> esDocList) {
        this.esDocList = esDocList;
        return this;
    }

    public long getTotalHits() {
        return totalHits;
    }

    public EsReaderResult setTotalHits(long totalHits) {
        this.totalHits = totalHits;
        return this;
    }

    public long getTook() {
        return took;
    }

    public EsReaderResult setTook(long took) {
        this.took = took;
        return this;
    }

    public double getSuccessRate() {
        return successRate;
    }

    public EsReaderResult setSuccessRate(double successRate) {
        this.successRate = successRate;
        return this;
    }

    public String getScrollId() {
        return scrollId;
    }

    public void setScrollId(String scrollId) {
        this.scrollId = scrollId;
    }

    @Override
    public String toString() {
        return "EsReaderResult{" +
                "esDocList=" + esDocList +
                ", totalHits=" + totalHits +
                ", took=" + took +
                ", successRate=" + successRate +
                ", scrollId=" + scrollId +
                '}';
    }
}
