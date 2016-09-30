package shy.sparkcore

import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
 * Created by root on 2015/12/24.
 */
object ScalaAccumulator extends App {

  val conf = new SparkConf().setAppName("ScalaAccumulator").setMaster("local")
  val sc = new SparkContext(conf)
  private val acc: Accumulator[Int] = sc.accumulator(0, "accumulator")
  sc.parallelize(Array(1, 2, 3, 4, 5)).foreach(num => acc += num)
  println(acc)
}
