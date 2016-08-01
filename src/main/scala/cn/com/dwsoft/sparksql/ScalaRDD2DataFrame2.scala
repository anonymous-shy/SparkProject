package cn.com.dwsoft.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
 * Created by root on 2015/12/30.
 * 通过动态转化的方式将RDD转化为DataFrame，较常用，直接通过字段类型和个数映射
 */
object ScalaRDD2DataFrame2 extends App {

  val conf = new SparkConf().setAppName("ScalaRDD2DataFrame2").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  //将文本转化为Row的普通RDD
  private val rowRDD: RDD[Row] = sc.textFile("hdfs://10.13.244.41:9000/spark/star").map(_.split(","))
    .map(arr => Row(arr(0).toInt, arr(1), arr(2).toInt))

  //构造动态元数据
  private val structType: StructType = StructType(Array(
    StructField("id", IntegerType, true),
    StructField("name", StringType, true),
    StructField("age", IntegerType, true)
  ))
  structType

  //将RDD根据元数据转成DataFrame,在注册成临时表
  sqlContext.createDataFrame(rowRDD, structType).registerTempTable("stars")

  //执行sql
  private val sql: DataFrame = sqlContext.sql("select * from stars where age > 25")

  //将DataFrame转成RDD并打印
  sql.rdd.foreach(row => println(row))
}
