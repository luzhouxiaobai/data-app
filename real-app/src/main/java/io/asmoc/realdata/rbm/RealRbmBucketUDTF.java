package io.asmoc.realdata.rbm;


import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

@FunctionHint(output = @DataTypeHint("ROW(bucketId INT, uid STRING)"))
public class RealRbmBucketUDTF extends TableFunction<Row> {

    public void eval(int bucketSize, String userId) {

        // 获取用户号
        long userNo = Long.parseLong(userId.substring(4));

        // 桶个数最小取10
        bucketSize = Math.max(bucketSize, 10);

        // 分桶
        int bucketId = (int) (userNo % bucketSize);

        // 分桶后uid
        String uid = String.valueOf(userNo / bucketSize);

        collect(Row.of(bucketId, uid));
    }

}
