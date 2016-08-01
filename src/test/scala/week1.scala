import scala.collection.mutable

/**
  * Created by Shy on 2016/7/10.
  */
object Week1 extends App {

  def signum(x: Int) = {
    if (x > 0)
      1
    else if (x == 0)
      0
    else -1
  }

  //  println(signum(-2))
  //
  //  for (i <- 0 to 10 reverse)
  //    println(i)

  def countdown(n: Int): Unit = {
    for (i <- 0 until n reverse)
      println(i)
  }

  countdown(9)

  def wordcount(line: String) = {
    val counts = mutable.Map[String, Int]()
    for (word <- line.split(" ")) {
      if (counts.contains(word))
        counts(word) = counts(word) + 1
      else
        counts += (word -> 1)
    }
    counts.toMap
  }
}
