package io.asmoc.realdata.rbm;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import io.asmoc.common.algorithm.rbm.RoaringBitMap;
import org.apache.flink.table.functions.AggregateFunction;

import java.io.IOException;

@FunctionHint(accumulator = @DataTypeHint(value = "RAW", bridgedTo = RoaringBitMap.class),
                output = @DataTypeHint("STRING"))
public class RealRbmDistinctUDAF extends AggregateFunction<String, RoaringBitMap> {

    // 返回最终计算结果
    @Override
    public String getValue(RoaringBitMap rbm) {
        try {
            return rbm.getRbmStr();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // 创建中间状态累加器
    @Override
    public RoaringBitMap createAccumulator() {
        return new RoaringBitMap();
    }

    // 中间状态计算逻辑
    public void accumulate(RoaringBitMap rbm, String toDistinctItem) {
        int intItem = Integer.parseInt(toDistinctItem);
        rbm.add(intItem);
    }

    // 回撤逻辑，用于回撤场合
    public void retract(RoaringBitMap rbm, String toDistinctItem) {
        int intItem = Integer.parseInt(toDistinctItem);
        rbm.remove(intItem);
    }

    // 实现该方法，flink会将聚合逻辑优化为两阶段聚合
    public void merge(RoaringBitMap rbm, Iterable<RoaringBitMap> it) {
        for (RoaringBitMap toRbm : it) {
            rbm.merge(toRbm);
        }
    }

}
