package shy.sparksql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 2015/12/28.
 * 使用反射方式将RDD转化为DataFrame
 * 但是这种方式通常需要先建立文件映射类，在日常中不太适用
 */
object ScalaRDD2DataFrame extends App {

  val conf = new SparkConf().setAppName("ScalaRDD2DataFrame").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  //Scala中使用反射的方式进行RDD到DataFrame的转换,需要手动导入一个隐式转换

  import sqlContext.implicits._

  case class Star(id: Integer, name: String, age: Integer)

  //通过case class将读入数据直接转变为RDD
  private val map: RDD[Star] = sc.textFile("hdfs://10.13.244.41:9000/spark/star").map(_.split(","))
    .map(arr => Star(arr(0).toInt, arr(1), arr(2).toInt))

  //将RDD转为DataFrame
  private val dataFrame: DataFrame = map.toDF()

  dataFrame.registerTempTable("stars")

  /**
  // 在scala中，对row的使用，比java中的row的使用，更加丰富
  // 在scala中，可以用row的getAs()方法，获取指定列名的列
  teenagerRDD.map { row => Student(row.getAs[Int]("id"), row.getAs[String]("name"), row.getAs[Int]("age")) }
      .foreach { stu => println(stu.id + ":" + stu.name + ":" + stu.age) }

  // 还可以通过row的getValuesMap()方法，获取指定几列的值，返回的是个map
  val studentRDD = teenagerRDD.map { row => {
      val map = row.getValuesMap[Any](Array("id", "name", "age"));
      Student(map("id").toString().toInt, map("name").toString(), map("age").toString().toInt)
    }
    */
  sqlContext.sql("select * from stars").rdd
    .foreach(row => println(row))
}
