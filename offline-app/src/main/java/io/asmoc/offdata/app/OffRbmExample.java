package io.asmoc.offdata.app;

import io.asmoc.offdata.spark.rbm.OffRbmCardinalitySUDAF;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

import io.asmoc.offdata.spark.rbm.OffRbmDistinctSUDAF;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class OffRbmExample {

    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession
                .builder()
                .config("spark.master", "local")
                .appName("UDF Test")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        List<String> userIdList = Arrays.asList("1222123123123", "1220121212321", "1220123123654");

        // Spark没有自定义UDTF，这里使用了一个FlatMapFunction
        JavaRDD<Row> stringJavaRDD = sc.parallelize(userIdList).flatMap(new FlatMapFunction<String, Tuple2<Integer, String>>() {
            @Override
            public Iterator<Tuple2<Integer, String>> call(String userId) throws Exception {
                int bucketSize = 10;
                long userNo = Long.parseLong(userId.substring(4));
                // 分桶
                int bucketId = (int) (userNo % bucketSize);
                // 分桶后uid
                String uid = String.valueOf(userNo / bucketSize);
                return Collections.singletonList(new Tuple2<Integer, String>(bucketId, uid)).iterator();
            }
        }).map(x -> {
            return RowFactory.create(x._1, x._2);
        });

        StructType schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("bucket_id", DataTypes.IntegerType, true),
                DataTypes.createStructField("uid", DataTypes.StringType, true)
        ));

        Dataset<Row> dataFrame = spark.createDataFrame(stringJavaRDD, schema);
        dataFrame.persist();

        System.out.println("init data show: ");
        dataFrame.show();

        spark.udf().register("rbm_distinct", functions.udaf(new OffRbmDistinctSUDAF(), Encoders.STRING()));

        dataFrame.createTempView("table_1");
        Dataset<Row> rbm = spark.sql("select bucket_id, rbm_distinct(uid) as rbm_uv from table_1 group by bucket_id");
        rbm.persist();
        System.out.println("rbm string data show: ");
        rbm.show();

        rbm.createTempView("table_2");
        spark.udf().register("rbm_cardinality", functions.udaf(new OffRbmCardinalitySUDAF(), Encoders.STRING()));
        System.out.println("uv data show: ");
        spark.sql("select sum(uv1) as uv2 from ( " +
                "select bucket_id, rbm_cardinality(rbm_uv) as uv1 from table_2 group by bucket_id) tmp").show();


    }

}
