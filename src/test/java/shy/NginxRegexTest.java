package shy;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by AnonYmous_shY on 2016/8/26.
 */
public class NginxRegexTest {

    public static void main(String[] args) {

        String log = "10.171.47.71 - - [19/Aug/2016:00:00:04 +0800] \"GET /news/set_readcount?type=3&id=2936929 HTTP/1.1\" 200 54 \"http://game.donews.com/201608/2936929.shtm\" \"Mozilla/5.0 (Windows NT 10.0; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0\" \"112.224.21.194, 182.92.7.14\"" ;
        String pattern = "([^ ]*) (-|[^ ]*) (-|[^ ]*) (\\[[^\\]]*\\]) (\"[^\"]*\") (-|[0-9]*) (-|[0-9]*) (\"-\"|\"[^\"]*\") (\"[^\"]*\") (\"[^\"]*\")";

        Pattern compile = Pattern.compile(pattern);
        Matcher matcher = compile.matcher(log);
        while (matcher.find()) {
            System.out.println(matcher.group(0));
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
            System.out.println(matcher.group(3));
            System.out.println(matcher.group(4));
            System.out.println(matcher.group(5));
            System.out.println(matcher.group(6));
            System.out.println(matcher.group(7));
            System.out.println(matcher.group(8));
            System.out.println(matcher.group(9));
            System.out.println(matcher.group(10));
        }
    }
}
