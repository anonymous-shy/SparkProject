

/**
  * Created by AnonYmous_shY on 2016/8/26.
  */
object NginxRegex extends App {

  val logs = "10.171.47.71 - - [19/Aug/2016:00:00:04 +0800] \"GET /news/set_readcount?type=3&id=2936929 HTTP/1.1\" 200 54 \"http://game.donews.com/201608/2936929.shtm\" \"Mozilla/5.0 (Windows NT 10.0; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0\" \"112.224.21.194, 182.92.7.14\""
  val regex = "([^ ]*) (-|[^ ]*) (-|[^ ]*) (\\[[^\\]]*\\]) (\"[^\"]*\") (-|[0-9]*) (-|[0-9]*) (\"-\"|\"[^\"]*\") (\"[^\"]*\") (\"[^\"]*\")".r

  val regex(remote_addr, dash, cookie_bdshare_firstime, time_local, request, status, body_bytes_sent, http_referer, http_user_agent, http_x_forwarded_for) = logs
  println(s"$remote_addr : $dash : $cookie_bdshare_firstime :  $time_local : " +
    s"$request : $status : $body_bytes_sent")
  println(s"$http_referer")
  println(http_user_agent)
  println(http_x_forwarded_for)
}
