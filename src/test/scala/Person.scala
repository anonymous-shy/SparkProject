/**
  * Created by Shy on 2016/7/10.
  */
class Person {

  var name: String = _  //自动生成getter/setter
  val age: Int = 18 //只生成getter
  private[this] val gender: String = "male"
}

object Person {

}

object Test extends App {


}
