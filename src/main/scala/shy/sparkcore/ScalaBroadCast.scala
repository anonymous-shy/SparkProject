package shy.sparkcore

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by root on 2015/12/24.
 */
object ScalaBroadCast extends App {

  val conf = new SparkConf().setAppName("ScalaBroadCast").setMaster("local")
  val sc = new SparkContext(conf)
  val factor = 3
  private val broadcast: Broadcast[Int] = sc.broadcast(factor)
  val arrs = Array(1, 2, 3, 4, 5)
  private val nums: RDD[Int] = sc.parallelize(arrs)
  nums.map(_ * broadcast.value).foreach(x => println(x))
}
