package io.asmoc.offdata.hive.rbm;


import io.asmoc.common.algorithm.rbm.RoaringBitMap;
import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

import java.io.IOException;

public class OffRbmCardinalityUDAF extends UDAF {

    public static class OffRbmCardinalityEvaluator implements UDAFEvaluator {

        private RoaringBitMap rbm;

        // 初始化
        @Override
        public void init() {
            rbm = new RoaringBitMap();
        }

        // 处理每一行输入数据。
        public boolean iterate(String rbmStr) {
            try {
                RoaringBitMap rbm = RoaringBitMap.getRbm(rbmStr);
                rbm.merge(rbm);
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            return true;
        }

        // 返回部分聚合结果，用于MapReduce的Map端
        public String terminatePartial() throws IOException {
            return rbm.getRbmStr();
        }

        // 合并多个部分聚合结果
        public boolean merge(String rbmStr) throws IOException, ClassNotFoundException {
            RoaringBitMap rbm1 = RoaringBitMap.getRbm(rbmStr);
            rbm.merge(rbm1);
            return true;
        }

        // 返回最终聚合结果
        public int terminate() {
            return rbm.getCardinality();
        }
    }

}
