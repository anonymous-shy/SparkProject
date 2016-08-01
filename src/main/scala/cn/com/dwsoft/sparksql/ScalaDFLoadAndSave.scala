package cn.com.dwsoft.sparksql

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 2015/12/31.
 * 通过load直接生成DataFrame
 */
object ScalaDFLoadAndSave extends App {

  val conf = new SparkConf().setAppName("ScalaDFLoadAndSave").setMaster("local")
  val sc = new SparkContext(conf)
  private val sqlContext: SQLContext = new SQLContext(sc)
  private val dataFrame: DataFrame = sqlContext.read.load("hdfs://10.13.244.41:9000/spark/users.parquet")
  dataFrame.show()
}
