package shy.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by root on 2015/12/17.
 */
object GroupTopK extends App {

  val conf = new SparkConf().setAppName("GroupTopK").setMaster("local")
  val sc = new SparkContext(conf)
  val text =
    sc.textFile("hdfs://10.13.244.41:9000/spark/sort.input")
  /**
   * 结果用于返回排序后的最大的三个值
   */
  val groupByKey: RDD[(String, Iterable[Int])] = text.map(_.split("\t")).
    map(x => (x(0), x(1).toInt)).groupByKey()

  val res_map = groupByKey.map(
    x => {
      val key = x._1
      val value = x._2.toList.sorted
      (key, value.reverse.take(3))
    }
  )
  groupByKey.foreach(x => println(x._1 + " : " + x._2))
  res_map.foreach(x => println(x._1 + " : " + x._2))

}
