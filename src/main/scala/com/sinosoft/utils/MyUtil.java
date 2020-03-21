package com.sinosoft.utils;

import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by guo on 2017/6/23.
 * 工具类
 */
public class MyUtil {

    //将string转换成list
    public static List<String> stringToList(String tmp) {
        List<String> list = new ArrayList<String>();
        if (tmp != null && !tmp.equals("")) {
            String[] fields = tmp.split(" ");
            for (int j = 0; j < fields.length; j++) {
                if (fields[j].matches("[\\u4e00-\\u9fa5]{1,}")) {
                    list.add(fields[j]);
                } else {
                    continue;
                }
            }
        }
        return list;
    }

    //将string时间格式转换成Timestamp
    public static Timestamp toTimestamp(String time) {
        Timestamp ts = Timestamp.valueOf(time);
        return ts;
    }

    //返回指定日期的前一天零点的long类型时间戳
    public static long startTimeToLong_yesterday(String time) {
        String[] fields = time.split(" ");
        long startTimeLong = timeTransformation(fields[0]);
        return startTimeLong - 86400000;
    }

    //返回指定日期的前一天23点的long类型时间戳
    public static long endTimeToLong_yesterday(String time) {
        String[] fields = time.split(" ");
        String timetmp = fields[0] + " 23:59:59";
        long startTimeLong = timeTransformation(timetmp);
        return startTimeLong - 86400000;
    }

    //返回指定日期的当天零点的long类型时间戳
    public static long startTimeToLong_day(String time) {
        String[] fields = time.split(" ");
        long startTimeLong = timeTransformation(fields[0]);
        return startTimeLong;
    }

    //返回指定日期的三天前的long类型时间戳
    public static long startTimeToLong_3day(String time) {
        String[] fields = time.split(" ");
        long startTimeLong = timeTransformation(fields[0]);
        return startTimeLong - 259200000;
    }

    //返回指定日期的上周的最后一天
    public static long getLastDayOfLastWeek(String time) {
        String[] fields = time.split(" ");
        String endTime = fields[0] + " " + "23:59";
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        long endTimeLong = 0;
        try {
            Date date = sf.parse(endTime);
            Calendar calendar = Calendar.getInstance();
            calendar.setFirstDayOfWeek(calendar.MONTH);//将每周第一天设为星期一，默认是星期天
            calendar.setTime(date);
            calendar.add(Calendar.DATE, -1 * 7);
            calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
            Date endTimeDate = calendar.getTime();
            endTimeLong = endTimeDate.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return endTimeLong;
    }

    //返回指定日期的上周的第一天
    public static long getFirstDayOfLastWeek(String time) {
        String[] fields = time.split(" ");
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
        long startTimeLong = 0;
        try {
            Date date = sf.parse(fields[0]);
            Calendar calendar = Calendar.getInstance();
            calendar.setFirstDayOfWeek(calendar.MONTH);//将每周第一天设为星期一，默认是星期天
            calendar.setTime(date);
            calendar.add(Calendar.DATE, -1 * 7);
            calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
            Date endTimeDate = calendar.getTime();
            startTimeLong = endTimeDate.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return startTimeLong;
    }

    //返回指定日期的上个月的最后一天
    public static long getLastDayOfLastMonth(String time) {
        String[] fields = time.split(" ");
        String endTime = fields[0] + " " + "23:59";
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        long endTimeLong = 0;
        try {
            Date date = sf.parse(endTime);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.set(calendar.get(Calendar.YEAR),
                    calendar.get(Calendar.MONTH) - 1, 1);
            calendar.roll(Calendar.DATE, -1);
            Date endTimeDate = calendar.getTime();
            endTimeLong = endTimeDate.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return endTimeLong;
    }

    //返回指定日期的上个月的第一天
    public static long getFirstDayOfLastMonth(String time) {
        String[] fields = time.split(" ");
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
        long startTimeLong = 0;
        try {
            Date date = sf.parse(fields[0]);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.set(calendar.get(Calendar.YEAR),
                    calendar.get(Calendar.MONTH) - 1, 1);
            calendar.roll(Calendar.DATE, 0);
            Date endTimeDate = calendar.getTime();
            startTimeLong = endTimeDate.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return startTimeLong;
    }

    //根据传入的时间计算前一年的开始时间
    public static long getFirstDayOfLastYear(String time) {
        String[] fields = time.split(" ");
        String year = fields[0].split("\\-")[0];
        String startTime = Integer.parseInt(year) - 1 + "-" + "01-01";
        long startTimeLong = timeTransformation(startTime);
        return startTimeLong;
    }

    //根据传入的时间计算前一年的结束时间
    public static long getLastDayOfLastYear(String time) {
        String[] fields = time.split(" ");
        String year = fields[0].split("\\-")[0];
        String endTime = Integer.parseInt(year) - 1 + "-" + "12-30 23:59";
        long endTimeLong = timeTransformation(endTime);
        return endTimeLong;
    }

    //将string类型的时间转换long类型时间戳
    public static long timeTransformation(String time) {
        long timeStemp = 0;
        if (time != null && !time.equals("")) {
            if (time.matches("\\d{4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}")) {
                SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = null;
                try {
                    date = sf.parse(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                timeStemp = date.getTime();
                return timeStemp;
            } else if (time.matches("\\d{4}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}")) {
                SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                Date date = null;
                try {
                    date = sf.parse(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                timeStemp = date.getTime();
                return timeStemp;
            } else if (time.matches("\\d{4}-\\d{1,2}-\\d{1,2}")) {
                SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
                Date date = null;
                try {
                    date = sf.parse(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                timeStemp = date.getTime();
                return timeStemp;
            } else if (time.matches("\\d{4}年\\d{1,2}月\\d{1,2}日\\d{1,2}:\\d{1,2}:\\d{1,2}")) {
                SimpleDateFormat sf = new SimpleDateFormat("yyyy年MM月dd日HH:mm:ss");
                Date date = null;
                try {
                    date = sf.parse(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                timeStemp = date.getTime();
                return timeStemp;
            } else if (time.matches("\\d{4}年\\d{1,2}月\\d{1,2}日\\d{1,2}:\\d{1,2}")) {
                SimpleDateFormat sf = new SimpleDateFormat("yyyy年MM月dd日HH:mm");
                Date date = null;
                try {
                    date = sf.parse(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                timeStemp = date.getTime();
                return timeStemp;
            } else if (time.matches("\\d{4}年\\d{1,2}月\\d{1,2}日")) {
                SimpleDateFormat sf = new SimpleDateFormat("yyyy年MM月dd日");
                Date date = null;
                try {
                    date = sf.parse(time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                timeStemp = date.getTime();
                return timeStemp;
            } else if (time.matches("\\d{4}-\\d{1,2}-\\d{1,2}T\\d{1,2}:\\d{1,2}:\\d{1,2}Z")) {
                SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = null;
                try {
                    date = sf.parse(time.replace("T", " ").replace("Z", ""));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                timeStemp = date.getTime();
                return timeStemp;
            } else if (time.matches("\\d{0,}")) {
                if (time.length() == 10) {
                    timeStemp = Long.parseLong(time) * 1000;
                } else if (time.length() == 13) {
                    timeStemp = Long.parseLong(time);
                }
                return timeStemp;
            } else if (time.matches("[a-zA-Z]{3}-\\d{1,2}-\\d{1,2} \\d{1,2}:\\d{1,2}:\\d{1,2}")) {
                String[] fields = time.split(" ");
                String time_tmp = fields[1];
                String date_tmp = fields[0];
                String[] fields_date = date_tmp.split("-");
                String year = "20" + fields_date[2];
                String month_tmp = fields_date[0];
                String day = fields_date[1];
                String month = "";
                if (month_tmp.equals("Jan")) {
                    month = "01";
                } else if (month_tmp.equals("Feb")) {
                    month = "02";
                } else if (month_tmp.equals("Mar")) {
                    month = "03";
                } else if (month_tmp.equals("Apr")) {
                    month = "04";
                } else if (month_tmp.equals("May")) {
                    month = "05";
                } else if (month_tmp.equals("Jun")) {
                    month = "06";
                } else if (month_tmp.equals("Jul")) {
                    month = "07";
                } else if (month_tmp.equals("Aug")) {
                    month = "08";
                } else if (month_tmp.equals("Sep")) {
                    month = "09";
                } else if (month_tmp.equals("Oct")) {
                    month = "10";
                } else if (month_tmp.equals("Nov")) {
                    month = "11";
                } else if (month_tmp.equals("Dec")) {
                    month = "12";
                }
                String result_time = year + "-" + month + "-" + day + " " + time_tmp;
                SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date date = null;
                try {
                    date = sf.parse(result_time);
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                timeStemp = date.getTime();
                return timeStemp;
            }
        }
        return timeStemp;
    }

    //过滤掉不在某一段时间范围内的记录
    public static boolean timeFilter(long startTime, long time, long endTime) {
        if (time >= startTime && time <= endTime) {
            return true;
        }
        return false;
    }

    //利用MD5进行加密
    public static String getMD5(String str) {
        String result = "";
        try {
            // 生成一个MD5加密计算摘要
            MessageDigest md = MessageDigest.getInstance("MD5");
            // 计算md5函数
            md.update(str.getBytes());
            // digest()最后确定返回md5 hash值，返回值为8为字符串。因为md5 hash值是16位的hex值，实际上就是8位的字符
            // BigInteger函数则将8位的字符串转换成16位hex值，用字符串来表示；得到字符串形式的hash值
            result = new BigInteger(1, md.digest()).toString(16);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    //获取地区编码
    public static String getRecordCode(String placeName){
        String recordCode = "Undisclosed";
        if(placeName != null && !placeName.equals("")){
            if(placeName.equals("京") || placeName.equals("北京市")){
                recordCode = "110000";
            }else if(placeName.equals("津") || placeName.equals("天津市")){
                recordCode = "120000";
            }else if(placeName.equals("冀") || placeName.equals("河北省")){
                recordCode = "130000";
            }else if(placeName.equals("晋") || placeName.equals("山西省")){
                recordCode = "140000";
            }else if(placeName.equals("蒙") || placeName.equals("内蒙古自治区")){
                recordCode = "150000";
            }else if(placeName.equals("辽") || placeName.equals("辽宁省")){
                recordCode = "210000";
            }else if(placeName.equals("吉") || placeName.equals("吉林省")){
                recordCode = "220000";
            }else if(placeName.equals("黑") || placeName.equals("黑龙江省")){
                recordCode = "230000";
            }else if(placeName.equals("沪") || placeName.equals("上海市")){
                recordCode = "310000";
            }else if(placeName.equals("苏") || placeName.equals("江苏省")){
                recordCode = "320000";
            }else if(placeName.equals("浙") || placeName.equals("浙江省")){
                recordCode = "330000";
            }else if(placeName.equals("皖") || placeName.equals("安徽省")){
                recordCode = "340000";
            }else if(placeName.equals("闽") || placeName.equals("福建省")){
                recordCode = "350000";
            }else if(placeName.equals("赣") || placeName.equals("江西省")){
                recordCode = "360000";
            }else if(placeName.equals("鲁") || placeName.equals("山东省")){
                recordCode = "370000";
            }else if(placeName.equals("豫") || placeName.equals("河南省")){
                recordCode = "410000";
            }else if(placeName.equals("鄂") || placeName.equals("湖北省")){
                recordCode = "420000";
            }else if(placeName.equals("湘") || placeName.equals("湖南省")){
                recordCode = "430000";
            }else if(placeName.equals("粤") || placeName.equals("广东省")){
                recordCode = "440000";
            }else if(placeName.equals("桂") || placeName.equals("广西壮族自治区")){
                recordCode = "450000";
            }else if(placeName.equals("琼") || placeName.equals("海南省")){
                recordCode = "460000";
            }else if(placeName.equals("渝") || placeName.equals("重庆市")){
                recordCode = "500000";
            }else if(placeName.equals("川") || placeName.equals("蜀") || placeName.equals("四川省")){
                recordCode = "510000";
            }else if(placeName.equals("黔") || placeName.equals("贵") || placeName.equals("贵州省")){
                recordCode = "520000";
            }else if(placeName.equals("滇") || placeName.equals("云") || placeName.equals("云南省")){
                recordCode = "530000";
            }else if(placeName.equals("藏") || placeName.equals("西藏自治区")){
                recordCode = "540000";
            }else if(placeName.equals("陕") || placeName.equals("秦") || placeName.equals("陕西省")){
                recordCode = "610000";
            }else if(placeName.equals("甘") || placeName.equals("陇") || placeName.equals("甘肃省")){
                recordCode = "620000";
            }else if(placeName.equals("青") || placeName.equals("青海省")){
                recordCode = "630000";
            }else if(placeName.equals("宁") || placeName.equals("宁夏回族自治区")){
                recordCode = "640000";
            }else if(placeName.equals("新") || placeName.equals("新疆维吾尔自治区")){
                recordCode = "650000";
            }else if(placeName.equals("台") || placeName.equals("台湾省")){
                recordCode = "710000";
            }else if(placeName.equals("港") || placeName.equals("香港特别行政区")){
                recordCode = "810000";
            }else if(placeName.equals("澳") || placeName.equals("澳门特别行政区")){
                recordCode = "820000";
            }
        }
        return recordCode;
    }
    //获取域名
    public static String domain(String str){
        String domain = "";
        try {
            URL url = new URL(str);
            domain = url.getProtocol()+"://"+url.getHost();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return domain;
    }

    //获取一级域名
    public static String first_domain(String str){
        String domain = "";
        /*String RE_TOP = "[\\w-]+\\.(com|cn|co|net|c|ne|or|hk|tv|me|wan|wang|bi|jp|wor|fr|s|so|club|ru|im|r|xn|m|xin|ru|ren|fi|t|blog|com.cn|net.cn|org|top|xyz|cx|red|edu|mil|name|mobi|org.cn|gov|cc|gov.cn|org\\.nz|biz|info)\\b()*";
        try {
            Pattern pattern = Pattern.compile(RE_TOP , Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(str);
            matcher.find();
            domain = matcher.group();
        } catch (Exception e) {
            System.out.println("[getTopDomain ERROR]====>");
        }*/
        try {
            URL url = null;
            if(str.contains("http")){
                url = new URL(str);
            }else{
                url = new URL("http://"+str);
            }
            String tmp = url.getHost();
            String[] split = tmp.split("\\.");
            if(split.length <= 2){
                domain = tmp;
            }else {
                domain = tmp.substring(tmp.indexOf(".")+1);
            }

        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        return domain;
    }
}
