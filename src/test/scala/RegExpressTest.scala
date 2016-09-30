import scala.util.matching.Regex
/**
  * Created by AnonYmous_shY on 2016/8/26.
  */
object RegExpressTest extends App {

  //匹配字母或数字
  val regex = """([0-9]+) ([a-z]+)""".r
  val content = "123 scala"
  val regex(num,str) = content
  println(num +":" + str) //单个匹配，都得匹配上

  //findAllIn、findFirstIn等方法
  val content2 = "hello 123 scala123 456 scala"
  println("findFirstIn:" + regex.findFirstIn(content2))

  println("findAllIn:")
  val regex1 = new Regex("""([0-9]+) ([a-z]+)""")
  val content3 = "1 yy 34 tt 56s 77"
  val allIn = regex1.findAllIn(content3)
  for(regex1(num,str)<-allIn)
    println(num + ":" + str)

  //使用match
  println("使用match：")
  val regex2 = new Regex("""([0-9]+) ([a-z]+)""")
  val content4 = "123 yy"
  content4 match{
    case regex2(num,str) => println(num + "\t" + str)
    case _=> println("Not matched")
  }
}
