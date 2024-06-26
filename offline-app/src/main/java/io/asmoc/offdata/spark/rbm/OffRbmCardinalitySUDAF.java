package io.asmoc.offdata.spark.rbm;

import io.asmoc.common.algorithm.rbm.RoaringBitMap;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.IOException;

public class OffRbmCardinalitySUDAF extends Aggregator<String, RoaringBitMap, Integer> {


    @Override
    public RoaringBitMap zero() {
        return new RoaringBitMap();
    }

    @Override
    public RoaringBitMap reduce(RoaringBitMap b, String a) {
        try {
            RoaringBitMap rbm = RoaringBitMap.getRbm(a);
            b.merge(rbm);
            return b;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RoaringBitMap merge(RoaringBitMap b1, RoaringBitMap b2) {
        b1.merge(b2);
        return b1;
    }

    @Override
    public Integer finish(RoaringBitMap reduction) {
        return reduction.getCardinality();
    }

    @Override
    public Encoder<RoaringBitMap> bufferEncoder() {
        return Encoders.javaSerialization(RoaringBitMap.class);
    }

    @Override
    public Encoder<Integer> outputEncoder() {
        return Encoders.INT();
    }
}
