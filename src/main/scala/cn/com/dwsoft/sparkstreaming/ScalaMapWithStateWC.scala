package cn.com.dwsoft.sparkstreaming

import org.apache.spark.streaming.dstream.{MapWithStateDStream, DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{StateSpec, State, Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

import scala.reflect.ClassTag

/**
 * Created by root on 2016/4/20.
 */
object ScalaMapWithStateWC {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("ScalaUpdateStateByKeyWC").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(5))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val map: DStream[(String, Int)] = lines.flatMap(_.split(" ")).map((_, 1))

    /**
     * :: Experimental ::
     * Return a [[MapWithStateDStream]] by applying a function to every key-value element of
     * `this` stream, while maintaining some state data for each unique key. The mapping function
     * and other specification (e.g. partitioners, timeouts, initial state data, etc.) of this
     * transformation can be specified using [[StateSpec]] class. The state data is accessible in
     * as a parameter of type [[State]] in the mapping function.
     *
     * Example of using `mapWithState`:
     * {{{
     *    // A mapping function that maintains an integer state and return a String
     *    def mappingFunction(key: String, value: Option[Int], state: State[Int]): Option[String] = {
     *      // Use state.exists(), state.get(), state.update() and state.remove()
     *      // to manage state, and return the necessary string
     *    }
     *
     *    val spec = StateSpec.function(mappingFunction).numPartitions(10)
     *
     *    val mapWithStateDStream = keyValueDStream.mapWithState[StateType, MappedType](spec)
     * }}}
     *
     * spec          Specification of this transformation
     * StateType    Class type of the state data
     * MappedType   Class type of the mapped data
     */

    val mappingFunc = (key: String, value: Option[Int], state: State[Int]) => {
      // Use state.exists(), state.get(), state.update() and state.remove()
      val sum: Int = value.getOrElse(0) + state.getOption().getOrElse(0)
      state.update(sum)
      // to manage state, and return the necessary string
      (key, sum)
    }
    val spec: StateSpec[String, Int, Int, (String, Int)] = StateSpec.function(mappingFunc)
    map.mapWithState(spec)
    ssc.start()
    ssc.awaitTermination()
  }
}
