package cn.com.dwsoft.sparksql

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
 * Created by root on 2015/12/28.
 */
object ScalaDataFrameUse1 extends App {

  val conf = new SparkConf().setAppName("ScalaDataFrameUse1").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  private val dataFrame: DataFrame = sqlContext.read.json("hdfs://10.13.244.41:9000/spark/star.json")
  dataFrame.show()
  dataFrame.printSchema()
  dataFrame.select("name").show()
  dataFrame.select(dataFrame("name"), dataFrame("age") + 1).show()
  dataFrame.filter(dataFrame("age") > 25).show()
  dataFrame.groupBy("age").count().show()
}
