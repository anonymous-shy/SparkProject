package cn.com.dwsoft.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by root on 2015/12/15.
 */
object SparkWordCount extends App {

  val conf = new SparkConf().setAppName("SparkWordCount").setMaster("local")
  //    .setMaster("spark://study1:7077")
  val sc = new SparkContext(conf)
  val text = sc.textFile("hdfs://10.13.244.41:9000/spark/wc.input")
  val reduceByKey: RDD[(String, Int)] = text.flatMap(_.split("\t")).map((_, 1)).reduceByKey(_ + _)
  reduceByKey.foreach(x => println(x._1 + " : " + x._2))
}
