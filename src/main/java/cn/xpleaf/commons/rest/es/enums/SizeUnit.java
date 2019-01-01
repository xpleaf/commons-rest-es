package cn.xpleaf.commons.rest.es.enums;

/**
 * @author xpleaf
 * @date 2019/1/1 5:56 PM
 *
 * 表示大小的单位
 */
public enum SizeUnit {

    TB("tb"),
    GB("gb"),
    MB("gb"),
    KB("kb"),
    BYTES("bytes");

    private final String unit;

    SizeUnit(String unit) {
        this.unit = unit;
    }

    public String getUnit() {
        return unit;
    }

    /**
     * 通过字符串获取到对应的SizeUnit
     * Note：enum本身也提供了valueOf()方法来获取字符串属性对应的enum，但是对于不存在常量对应的enum，
     * 其会抛出IllegalArgumentException异常，但是这里的方法则会返回null，这是区别
     * @param value 单位大小对应的字符串
     * @return 该字符串单位对应的unit
     */
    public static SizeUnit VALUE(String value) {
        for (SizeUnit sizeUnit : SizeUnit.values()) {
            if (sizeUnit.unit.equals(value)) {
                return sizeUnit;
            }
        }
        return null;
    }

/*    public static void main(String[] args) {
        System.out.println(SizeUnit.valueOf("kb").getUnit());
        System.out.println(SizeUnit.VALUE("gb").getUnit());
    }*/

}
