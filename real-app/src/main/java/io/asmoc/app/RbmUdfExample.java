package io.asmoc.app;

import io.asmoc.realdata.rbm.RealRbmCardinalityUDAF;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import io.asmoc.realdata.rbm.RealRbmBucketUDTF;
import io.asmoc.realdata.rbm.RealRbmDistinctUDAF;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class RbmUdfExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.createTemporarySystemFunction("rbm_bucket", RealRbmBucketUDTF.class);
        tableEnv.createTemporarySystemFunction("rbm_distinct", RealRbmDistinctUDAF.class);
        tableEnv.createTemporarySystemFunction("rbm_cardinality", RealRbmCardinalityUDAF.class);

        String host = "localhost";
        int port = 9999;

        DataStream<String> socketData = env.socketTextStream(host, port);

        Table socketTable = tableEnv.fromDataStream(socketData).as("user_id");

        tableEnv.createTemporaryView("example_table", socketTable);

        // 查询使用UDF
        Table resultTable = tableEnv.sqlQuery(
                "SELECT bucket, rbm_distinct(uid) AS uv " +
                    "FROM example_table," +
                    "LATERAL TABLE(rbm_bucket(10, user_id)) AS T(bucket, uid) " +
                    "GROUP BY bucket"
        );

//        tableEnv.createTemporaryView("add", resultTable);
//
//        Table table = tableEnv.sqlQuery("select rbm_cardinality(uv) as res from add");
//
//        Table rbmResultStage1 = resultTable
//                .groupBy($("bucket"))
//                .select($("bucket"), call("rbm_cardinality", $("uv")).as("uv_data"));
//
//        Table rbmResult = rbmResultStage1.select($("uv_data").sum().as("result"));

//         将结果转换为DataStream并打印
//        tableEnv.toAppendStream(resultTable, Row.class).print();

        tableEnv.toRetractStream(resultTable, Row.class).print();

//        tableEnv.toDataStream(table, Row.class).print();

        // 执行作业
        env.execute("Flink UDF Example");

    }

}
