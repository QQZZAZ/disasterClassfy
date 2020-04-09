package com.sinosoft.hbase;

import org.apache.commons.lang3.StringUtils;

import java.security.MessageDigest;

/**
 * Created by NGWaiming on 2018/2/8.
 */
public class MD5Utils {
    private static volatile MD5Utils instance;
    private final static String[] hexDigits = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"};

    private MD5Utils() {
    }

    public static MD5Utils getInstance() {
        if (instance == null) {
            synchronized (MD5Utils.class) {
                if (instance == null) {
                    instance = new MD5Utils();
                }
            }
        }
        return instance;
    }

    /**
     * 对字符串进行MD5编码
     *
     * @param originString
     * @param bit          需要返回的字符串位数[8,24,16,32]
     * @return bit位加密字符串
     * @note 16位加密字符串为截取32位加密字符串[8, 24]
     */
    public String encodeByMD5(String originString, Integer bit) {
        String result = "";
        if (StringUtils.isNotEmpty(originString)) {
            try {
                // 创建具有指定算法名称的信息摘要
                MessageDigest md5 = MessageDigest.getInstance("MD5");
                // 使用指定字节数组对摘要进行最后更新，然后完成摘要计算
                byte[] results = md5.digest(originString.toString().getBytes("UTF8"));
                if (bit == 32) {
                    result = byteArrayToHexString(results);
                } else if (bit == 16) {
                    result = byteArrayToHexString(results).substring(8, 24);
                } else if (bit == 8) {
                    result = byteArrayToHexString(results).substring(4, 12);
                } else if (bit == 24) {
                    result = byteArrayToHexString(results).substring(5, 29);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    /**
     * 轮换字节数组位十六进制字符串
     *
     * @param bytes 字节数组
     * @return 十六进制字符串
     */
    public static String byteArrayToHexString(byte[] bytes) {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < bytes.length; i++) {
            buffer.append(byteToHexString(bytes[i]));
        }
        return buffer.toString();
    }

    /**
     * 将一个字节转化成十六进制形式的字符串
     *
     * @param b
     * @return
     */
    private static String byteToHexString(byte b) {
        int n = b;
        if (n < 0) {
            n = 256 + n;
        }
        int d1 = n / 16;
        int d2 = n % 16;
        return hexDigits[d1] + hexDigits[d2];
    }

}
