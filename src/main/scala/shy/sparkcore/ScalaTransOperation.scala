package shy.sparkcore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by root on 2015/12/22.
 */
class ScalaTransOperation {

}

object mapOperation extends App {
  val conf = new SparkConf().setAppName("mapOperation").setMaster("local")
  val sc = new SparkContext(conf)
  val nums = Array(1, 2, 3, 4, 5)
  private val parallelize: RDD[Int] = sc.parallelize(nums)
  private val map: RDD[Int] = parallelize.map(_ * 2)
  map.foreach(x => println(x))
}

object filterOperation extends App {
  val conf = new SparkConf().setAppName("filterOperation").setMaster("local")
  val sc = new SparkContext(conf)
  val nums = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
  private val parallelize: RDD[Int] = sc.parallelize(nums)
  private val filter: RDD[Int] = parallelize.filter(_ % 2 == 0)
  filter.foreach(x => println(x))
}

object flatMapOperation extends App {
  val conf = new SparkConf().setAppName("flatMapOperation").setMaster("local")
  val sc = new SparkContext(conf)
  private val textFile: RDD[String] = sc.textFile("hdfs://10.13.244.41:9000/spark/wc.input")
  private val flatMap: RDD[String] = textFile.flatMap(_.split("\t"))
  flatMap.foreach(x => println(x))
}

object groupByKeyOperation extends App {
  val conf = new SparkConf().setAppName("groupByKeyOperation").setMaster("local")
  val sc = new SparkContext(conf)
  val scoreList = Array(Tuple2("Hadoop", 89),
    Tuple2("Spark", 95),
    Tuple2("Storm", 90),
    Tuple2("Hadoop", 60),
    Tuple2("Storm", 30),
    Tuple2("Spark", 50))
  private val parallelize: RDD[(String, Int)] = sc.parallelize(scoreList)
  private val groupByKey: RDD[(String, Iterable[Int])] = parallelize.groupByKey()
  groupByKey.foreach(score => {
    println(score._1)
    score._2.foreach(ss => println(ss))
  })
}

object reduceByKeyOperation extends App {
  val conf = new SparkConf().setAppName("groupByKeyOperation").setMaster("local")
  val sc = new SparkContext(conf)
  val scoreList = Array(Tuple2("Hadoop", 89),
    Tuple2("Spark", 95),
    Tuple2("Storm", 90),
    Tuple2("Hadoop", 60),
    Tuple2("Storm", 30),
    Tuple2("Spark", 50))
  private val parallelize: RDD[(String, Int)] = sc.parallelize(scoreList)
  parallelize.reduceByKey(_ + _).foreach(x => println(x._1 + " : " + x._2))
}

