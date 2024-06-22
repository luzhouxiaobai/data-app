package io.asmoc.realdata.rbm;

import io.asmoc.common.algorithm.rbm.RoaringBitMap;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.AggregateFunction;

import java.io.IOException;

@FunctionHint(
        accumulator = @DataTypeHint(value = "RAW", bridgedTo = RoaringBitMap.class),
        output = @DataTypeHint("INT")
)
public class RealRbmCardinalityUDAF extends AggregateFunction<Integer, RoaringBitMap> {
    @Override
    public Integer getValue(RoaringBitMap rbm) {
        return rbm.getCardinality();
    }

    @Override
    public RoaringBitMap createAccumulator() {
        return new RoaringBitMap();
    }

    public void accumulate(RoaringBitMap rbm, String rbmStr) throws IOException, ClassNotFoundException {
        RoaringBitMap rbmIn = RoaringBitMap.getRbm(rbmStr);
        rbm.merge(rbmIn);
    }

    // 回撤逻辑，用于回撤场合
    public void retract(RoaringBitMap rbm, String rbmStr) throws IOException, ClassNotFoundException {
        rbm.get().andNot(RoaringBitMap.getRbm(rbmStr).get());

    }

    // 实现该方法，flink会将聚合逻辑优化为两阶段聚合
    public void merge(RoaringBitMap rbm, Iterable<RoaringBitMap> it) {
        for (RoaringBitMap toRbm : it) {
            rbm.merge(toRbm);
        }
    }
}
