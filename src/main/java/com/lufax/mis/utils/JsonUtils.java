package com.lufax.mis.utils;

/**
 * Description: ${DESCRIPTION}
 *
 * @author sherlock
 * @create 2019/4/22
 * @since 1.0.0
 */
public class JsonUtils {

    public static String parseJson2String(){
        return null;
    }

    public static String subJsonString(String jsonString){
        int length = jsonString.length();
        return jsonString.substring(1,(length-1));
    }
}

class TestJsonUtils{

    public static void main(String[] args) {

        String jsonStr = "{\"name\":\"cat\",\"sex\":\"miao\"}";
        String string = JsonUtils.subJsonString(jsonStr);
        System.out.println(string);

    }
}
