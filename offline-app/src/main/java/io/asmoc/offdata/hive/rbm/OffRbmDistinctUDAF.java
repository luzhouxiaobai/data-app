package io.asmoc.offdata.hive.rbm;

import io.asmoc.common.algorithm.rbm.RoaringBitMap;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;

public class OffRbmDistinctUDAF implements GenericUDAFResolver {

    // 用于返回一个GenericUDAFEvaluator的实例。
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentLengthException("Exactly one argument is expected.");
        }
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE ||
                ((PrimitiveObjectInspector) parameters[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentException("Only double type arguments are accepted.");
        }
        return new RbmDistinctUDAFEvaluator();
    }

    public static class RbmDistinctUDAFEvaluator extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector inputOI;
        private RoaringBitMap result;

        //初始化方法，定义输入和输出的ObjectInspector。
        @Override
        public ObjectInspector init(Mode mode, ObjectInspector[] parameters) throws HiveException {
            super.init(mode, parameters);
            if (mode == Mode.PARTIAL1 || mode == Mode.COMPLETE) {
                inputOI = (PrimitiveObjectInspector) parameters[0];
            }
            result = new RoaringBitMap();
            return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
        }

        static class HiveRbm extends RoaringBitMap implements AggregationBuffer {

        }

        // 创建新的聚合缓冲区
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            HiveRbm agg = new HiveRbm();
            reset(agg);
            return agg;
        }

        // 重置聚合缓冲区。
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            HiveRbm myAgg = (HiveRbm) agg;
            myAgg.get().clear();
        }

        // 处理每一行输入数据。
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            HiveRbm myAgg = (HiveRbm) agg;
            if (parameters[0] != null) {
                String value = PrimitiveObjectInspectorUtils.getString(parameters[0], inputOI);
                myAgg.add(Integer.parseInt(value));
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            HiveRbm myAgg = (HiveRbm) agg;
            if (myAgg != null) {
                result.merge(myAgg);
            }
            return result;
        }

        //合并多个部分聚合结果。
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                HiveRbm myAgg = (HiveRbm) agg;
                HiveRbm p = (HiveRbm) partial;
                myAgg.merge(p);
            }
        }

        // 返回最终结果
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            try {
                return ((HiveRbm)agg).getRbmStr();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
