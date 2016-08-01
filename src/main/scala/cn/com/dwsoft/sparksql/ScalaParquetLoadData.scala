package cn.com.dwsoft.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by root on 2015/12/31.
 */
object ScalaParquetLoadData extends App {

  val conf = new SparkConf().setAppName("ScalaDFLoadAndSave").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  sqlContext.read.parquet("hdfs://10.13.244.41:9000/spark/users.parquet")
    .registerTempTable("users")
  sqlContext.sql("select name from users").rdd
    .foreach(row => println("name: " + row.getString(0)))
}
