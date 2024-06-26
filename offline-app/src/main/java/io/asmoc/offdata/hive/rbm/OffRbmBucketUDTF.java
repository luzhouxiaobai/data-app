package io.asmoc.offdata.hive.rbm;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;


public class OffRbmBucketUDTF extends GenericUDTF {

    private PrimitiveObjectInspector intOI;
    private PrimitiveObjectInspector stringOI;

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 2) {
            throw new UDFArgumentLengthException("The function off_rbm_bucket_udtf(int, string) takes exactly 2 arguments.");
        }

        if (!(args[0] instanceof PrimitiveObjectInspector) ||
                !(args[1] instanceof PrimitiveObjectInspector)) {
            throw new UDFArgumentTypeException(0, "Both arguments must be primitive types.");
        }

        intOI = (PrimitiveObjectInspector) args[0];
        stringOI = (PrimitiveObjectInspector) args[1];

        if (intOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
            throw new UDFArgumentTypeException(0, "The first argument must be an integer.");
        }

        if (stringOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(1, "The second argument must be a string.");
        }

        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldOIs = new ArrayList<>();

        fieldNames.add("int_field");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

        fieldNames.add("string_field");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        int bucketSize = (int) intOI.getPrimitiveJavaObject(args[0]);
        String userId = stringOI.getPrimitiveJavaObject(args[1]).toString();

        // 获取用户号
        long userNo = Long.parseLong(userId.substring(4));

        // 桶个数最小取10
        bucketSize = Math.max(bucketSize, 10);

        // 分桶
        int bucketId = (int) (userNo % bucketSize);

        // 分桶后uid
        String uid = String.valueOf(userNo / bucketSize);

        forward(new Object[]{bucketId, new Text(uid)});

    }

    @Override
    public void close() throws HiveException {
        // No cleanup necessary for this example
    }
}
