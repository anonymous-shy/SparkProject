package cn.com.dwsoft.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by root on 2015/12/25.
 */
public class DataFrameUse1 {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DataFrameUse1").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame dataFrame = sqlContext.read().json("hdfs://10.13.244.41:9000/spark/star.json");
        //DataFrame 常用操作
        //show() : select *
        dataFrame.show();
        //printSchema() : 打印DataFrame的元数据信息(Schema)
        dataFrame.printSchema();
        //查询某一列信息
        dataFrame.select("name").show();
        //查询某几列信息
        dataFrame.select(dataFrame.col("name"), dataFrame.col("age").plus(1)).show();
        //过滤
        dataFrame.filter(dataFrame.col("age").gt(25)).show();
        //分组
        dataFrame.groupBy(dataFrame.col("age")).count().show();
    }
}
