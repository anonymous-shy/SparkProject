package shy.sparkstreaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.{MapWithStateDStream, DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{State, StateSpec, Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by root on 2016/4/20.
 */
object ScalaForeachRDD {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("ScalaUpdateStateByKeyWC").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val stateDStream: MapWithStateDStream[String, Int, Int, (String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))
      .mapWithState(StateSpec.function((key: String, value: Option[Int], state: State[Int]) => {
      // Use state.exists(), state.get(), state.update() and state.remove()
      val sum: Int = value.getOrElse(0) + state.getOption().getOrElse(0)
      state.update(sum)
      // to manage state, and return the necessary string
      (key, sum)
    }))
    stateDStream.foreachRDD(rdd => {
      val sqlContext: SQLContext = SQLContextSingleton.getInstance(sc)
      val schemaString = "word cnt"
      val schema =
        StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
      val rowRDD: RDD[Row] = rdd.map(row => Row(row._1, row._2))
      val wordsCntDataFrame: DataFrame = sqlContext.createDataFrame(rowRDD, schema)
      wordsCntDataFrame.registerTempTable("words_cnt")
      sqlContext.sql("select word, cnt from words_cnt")
        .show()
    })



    ssc.start()
    ssc.awaitTermination()
  }
}

object SQLContextSingleton {
  @transient private var instance: SQLContext = _

  def getInstance(sparkContext: SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}