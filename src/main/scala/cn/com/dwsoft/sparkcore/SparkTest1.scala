package cn.com.dwsoft.sparkcore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by root on 2015/12/15.
 */
object SparkTest1 extends App {

  //val conf = new SparkConf().setAppName("Simple Application").setMaster("spark://study1:7077")
  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
  val logFile = "hdfs://10.13.244.41:9000/README.md"
  val sc = new SparkContext(conf)
  val logData = sc.textFile(logFile, 2).cache()
  val numSparks = logData.filter(line => line.contains("Spark")).count()
  val numBs = logData.filter(line => line.contains("b")).count()
  println("RowNumbers：" + logData.count())
  println("Lines with Spark: %s, Lines with b: %s".format(numSparks, numBs))
}
