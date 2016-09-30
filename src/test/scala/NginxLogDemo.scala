import scala.io.{BufferedSource, Source}

/**
  * Created by AnonYmous_shY on 2016/8/27.
  */
object NginxLogDemo extends App {

  private val file: BufferedSource = Source.fromFile("G:\\logs\\test.log")

  val regex = "([^ ]*) (-|[^ ]*) (-|[^ ]*) (\\[[^\\]]*\\]) (\"[^\"]*\") (-|[0-9]*) (-|[0-9]*) (\"-\"|\"[^\"]*\") (\"[^\"]*\") (\"[^\"]*\")".r
  //donews.access.log
  for (line <- Source.fromFile("G:\\logs\\test.log").getLines()) {
    println(line)
    val regex(remote_addr, dash, cookie_bdshare_firstime, time_local, request, status, body_bytes_sent, http_referer, http_user_agent, http_x_forwarded_for) = line
    val ip: String = http_x_forwarded_for.split(",")(0).substring(1)
    val ua: String = http_user_agent.split(" ")(0).substring(1)
    val cookie = s"$ip-$ua"
    println(s"$remote_addr $dash $cookie $time_local $request $status $body_bytes_sent $http_referer $http_user_agent $http_x_forwarded_for")
  }
}
