package shy;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by AnonYmous_shY on 2016/8/26.
 */
public class RegexTEst {

    public static void main(String[] args) {
        String s = "10.171.47.71 - \"Mozilla/5.0 (Windows NT 10.0; WOW64; rv:48.0) Gecko/20100101 Firefox/48.0\" \"112.224.21.194, 182.92.7.14\"";
        String p = "([^ ]*) (-|[^ ]*) (\"[^\"]*\") (\"[^\"]*\")";
        Pattern compile = Pattern.compile(p);
        Matcher matcher = compile.matcher(s);
        while (matcher.find()) {
            System.out.println(matcher.group());
        }
    }
}
