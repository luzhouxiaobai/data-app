package io.asmoc.offdata.spark.rbm;

import io.asmoc.common.algorithm.rbm.RoaringBitMap;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.expressions.Aggregator;

import java.io.IOException;

public class OffRbmDistinctSUDAF extends Aggregator<String, RoaringBitMap, String> {

    // initial buffer
    @Override
    public RoaringBitMap zero() {
        return new RoaringBitMap();
    }


    // accumulate a new input
    @Override
    public RoaringBitMap reduce(RoaringBitMap b, String a) {
        b.add(Integer.parseInt(a));
        return b;
    }


    @Override
    public RoaringBitMap merge(RoaringBitMap b1, RoaringBitMap b2) {
        b1.merge(b2);
        return b1;
    }

    // ouput
    @Override
    public String finish(RoaringBitMap reduction) {
        try {
            return reduction.getRbmStr();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Encoder<RoaringBitMap> bufferEncoder() {
        return Encoders.javaSerialization(RoaringBitMap.class);
    }

    @Override
    public Encoder<String> outputEncoder() {
        return Encoders.STRING();
    }
}
