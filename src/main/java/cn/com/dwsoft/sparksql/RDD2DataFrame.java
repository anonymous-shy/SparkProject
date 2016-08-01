package cn.com.dwsoft.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Created by root on 2015/12/28.
 * 使用反射方式将RDD转化为DataFrame
 * 但是这种方式通常需要先建立文件映射类，在日常中不太适用
 */
public class RDD2DataFrame {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RDD2DataFrame").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> textFile = sc.textFile("hdfs://10.13.244.41:9000/spark/star");
        JavaRDD<Star> starRDD = textFile.map(new Function<String, Star>() {
            @Override
            public Star call(String line) throws Exception {
                String[] split = line.split(",");
                Star star = new Star(Integer.valueOf(split[0]), split[1], Integer.valueOf(split[2]));
                return star;
            }
        });
        DataFrame starDF = sqlContext.createDataFrame(starRDD, Star.class);
        starDF.registerTempTable("stars");
        DataFrame sqlDF = sqlContext.sql("select * from stars where age > 25");
        JavaRDD<Row> rowJavaRDD = sqlDF.javaRDD();
        JavaRDD<Star> javaRDD = rowJavaRDD.map(new Function<Row, Star>() {
            @Override
            public Star call(Row row) throws Exception {
                Star star = new Star();
                star.setAge(row.getInt(0));
                star.setId(row.getInt(1));
                star.setName(row.getString(2));
                return star;
            }
        });
        javaRDD.foreach(new VoidFunction<Star>() {
            @Override
            public void call(Star star) throws Exception {
                System.out.println(star);
            }
        });
    }
}
