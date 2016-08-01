package cn.com.dwsoft.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by root on 2015/12/16.
 */
object WordCountSortBy extends App {

  val conf = new SparkConf().setAppName("WordCount2").setMaster("local")
  val sc = new SparkContext(conf)
  val textFile: RDD[String] = sc.textFile("hdfs://10.13.244.41:9000/spark/wc.input")
  val map: RDD[(String, Int)] = textFile.flatMap(_.split("\t")).
    map((_, 1)).reduceByKey(_ + _).map(x => (x._2, x._1)).sortByKey(false).map(x => (x._2, x._1))
  map.foreach(x => println(x._1 + " : " + x._2))
  map.foreach(println)
}