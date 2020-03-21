package com.sinosoft;

public class JavaTest {

    // date1,date2 是时间戳的Long值
    public static int differentDaysByMillisecond(Long date1, Long date2) {
        int days = (int) ((date2 - date1) / (1000 * 3600 * 24));
        return days;
    }

    public static void main(String[] args) throws InterruptedException {

    }
}
