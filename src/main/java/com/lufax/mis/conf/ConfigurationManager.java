package com.lufax.mis.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * 配置文件管理类
 *
 * @author sherlock
 * @create 2019/4/22
 * @since 1.0.0
 */
public class ConfigurationManager {

    private static Properties properties = new Properties();

    static {
        try {
            InputStream inputStream = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String key){
        return properties.getProperty(key);
    }

    public static int getIntegerPro(String Key) { return Integer.valueOf(properties.getProperty(Key)); }

    public static boolean getBooleanPro(String key) { return Boolean.valueOf(properties.getProperty(key)); }

    public static long getLongPro(String key) { return  Long.valueOf(properties.getProperty(key)); }

}

class TestConfigurationManager {

    public static void main(String[] args) {
        System.out.println(ConfigurationManager.class.getResource(""));
        System.out.println(ConfigurationManager.class.getResource("/"));
        System.out.println(ConfigurationManager.class.getClassLoader().getResource(""));
        System.out.println(ConfigurationManager.class.getClassLoader().getResource("/"));
    }
}