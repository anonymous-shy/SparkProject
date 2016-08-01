/**
 * Created by root on 2015/12/29.
 * scala模式匹配使用
 */
//针对变量进行匹配
object MatchTest extends App {

  def grade(grade: String): Unit = {
    grade match {
      case "A" => println("Excellent")
      case "B" => println("Good")
      case "C" => println("Just so so")
      case _ => println("Bad")
    }
  }

  grade("A")
  grade("B")
  grade("C")
  grade("D")

  //使用下划线代替变量,匹配不满足情况的变量
  def grade1(name: String, grade: String): Unit = {
    grade match {
      case "A" => println("Excellent")
      case "B" => println("Good")
      case "C" => println("Just so so")
      case "D" if name == "Leo" => println(name + " good good")
      case _ => println("Bad")
    }
  }

  grade1("Leo", "D")

  //使用变量代替下划线,可以取值
  def grade2(name: String, grade: String): Unit = {
    grade match {
      case "A" => println("Excellent")
      case "B" => println("Good")
      case "C" => println("Just so so")
      case "D" if name == "Leo" => println(name + " good good")
      case _grade => println(name + ",Bad " + _grade)
    }
  }

  grade2("Hah", "E")
}

object MatchArr extends App {

  def greet(arr: Array[String]): Unit = {
    arr match {
      case Array("Hah") => println("Hah")
      case Array(n1, n2, n3) => println(n1 + " " + n2 + " " + n3)
      case Array("Spark", _*) => println("Spark YE")
      case _ => println("XO")
    }
  }
}

object MatchClass extends App {

  case class Star(name: String, age: Integer)

  def test(star: Star) = star match {
    case Star(name, age) => println(name + " " + age)
    case _ => println("error")
  }

  test(Star("Hah", 18))
}
