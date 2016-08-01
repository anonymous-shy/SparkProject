package cn.com.dwsoft;

import org.junit.Test;

/**
 * Created by root on 2016/1/5.
 */
public class CharAt {

    @Test
    public void test1() {
        String s = "基本套餐106元月费（首月按量计费）";
        System.out.println(s.indexOf("元"));
    }
}
