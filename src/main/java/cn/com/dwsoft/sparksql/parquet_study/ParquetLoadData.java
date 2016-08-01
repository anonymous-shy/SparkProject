package cn.com.dwsoft.sparksql.parquet_study;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Created by root on 2015/12/31.
 */
public class ParquetLoadData {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DFLoadAndSave").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame dataFrame = sqlContext.read().parquet("hdfs://10.13.244.41:9000/spark/users.parquet");
        dataFrame.registerTempTable("users");
        DataFrame sql = sqlContext.sql("select name from users");
        sql.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println("name: " + row.getString(0));
            }
        });
    }
}
