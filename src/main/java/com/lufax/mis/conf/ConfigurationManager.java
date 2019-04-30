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


}
