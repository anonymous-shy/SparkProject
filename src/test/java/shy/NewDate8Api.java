package shy;

import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;

/**
 * Created by root on 2016/1/5.
 * Java 8 新时间API测试
 */
public class NewDate8Api {

    @Test
    public void test1() {
        LocalDate date = LocalDate.now();
        date.atStartOfDay(ZoneId.of("Asia/Shanghai")).toInstant();

        LocalTime time = LocalTime.now();
        System.out.println("DATE : " + date);
        System.out.println("TIME : " + time);
    }
}
