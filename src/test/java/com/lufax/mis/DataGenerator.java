package com.lufax.mis;

import com.google.common.collect.Maps;
import com.lufax.mis.utils.DateUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;

import java.security.Key;
import java.util.*;

/**
 * 模拟数据的生成类
 *
 * @author sherlock
 * @create 2019/4/19
 * @since 1.0.0
 */
public class DataGenerator {

    private static final Random RANDOM = new Random();
    private static final String PREFIXNAME = "LuAppUser";
    private static Map<Integer, String> userMap = Maps.newHashMap();

    private static String getUuid() {
        String uuid = UUID.randomUUID().toString();
        return "\"uuid\":" + "\"" + uuid + "\"";
    }

    private static Map<Integer, String> setUserIdAndName() {
        for (int i = 1; i < 11; i++) {
            userMap.put(i, PREFIXNAME + i);
        }
        return userMap;
    }

    private static String getUserIdAndUserName(int userId) {
        userMap = setUserIdAndName();
        String userName = userMap.get(userId);
        return "\"user_id\":" + "\"" + userId + "\"" + "," + "\"user_name\":" + "\"" + userName + "\"";
    }

    private static String getRequestTime(long timestamp) {
        String request_time = DateUtils.getTimestamp2DateStr(timestamp);
        return "\"request_time\":" + "\"" + timestamp + "\"";
    }

    private static String getSimplePointTypeAndCategory() {
        List<String> pointTypes = Arrays.asList("screen", "event");
        List<String> categories = Arrays.asList("home", "service", "find", "myAccount", "recharge");
        List<String> actions = Arrays.asList("home", "service", "find", "myAccount", "recharge");
        List<String> titles = Arrays.asList("channel", "message", "list", "bannel");
        String _pointType = pointTypes.get(RANDOM.nextInt(2));
        String _category = categories.get(RANDOM.nextInt(5));
        String _action = actions.get(RANDOM.nextInt(5));
        String _title = titles.get(RANDOM.nextInt(4));
        return "\"point_type\":" + "\"" + _pointType + "\"" + "," +
                "\"category\":" + "\"" + _category + "\"" + "," +
                "\"title\":" + "\"" + _title + "\"" + "," +
                "\"action\":" + "\"" + _action + "\"";
    }

    private static String getPointTypeAndCategory() {
        List<String> pointTypes = Arrays.asList("screen", "event");
        List<String> categories = Arrays.asList("home", "service", "find", "myAccount", "recharge");
        List<String> actions = Arrays.asList("home", "service", "find", "myAccount", "recharge");
        List<String> titles = Arrays.asList("channel", "message", "list", "bannel");
        String _pointType = pointTypes.get(RANDOM.nextInt(2));
        String _category = categories.get(RANDOM.nextInt(5));
        String _action = actions.get(RANDOM.nextInt(5));
        String _title = titles.get(RANDOM.nextInt(4));
        if (_pointType != null){
            if ("event".equals(_pointType)){
                return "\"point_type\":" + "\"event\"" + "," +
                        "\"category\":"+ "\"" + _category + "\"" + "," +
                        "\"title\":"+ "\"" + _title + "\"" + "," +
                        "\"action\":" + "\"" + _action + "\"" + "," +
                        "\"click\":" + "\"1\"";
            }else {
                return "\"point_type\":" + "\"screen\"" + "," +
                        "\"category\":"+ "\"" + _action + "\"" + "," +
                        "\"title\":"+ "\"" + _title + "\"" + "," +
                        "\"click\":" + "\"0\"";
            }
        }
        return null;
    }

    /*
            {"uuid":"59fa1fec-1746-4777-b206-14c8083f6782","user_id":"9","user_name":"LuAppUser9","request_time":"1564716723905","point_type":"screen","category":"home","title":"message","click":"0","is":" ","event_id":" ","platform":" ","ip":" ","lon":" ","lat":" ","ls":" ","product_id":" ","business_type":" ","business_id":" ","invest_id":" ","address":" ","type":" "}
            {"uuid":"c52bec66-4af2-470c-ae96-45924ebdc47c","user_id":"6","user_name":"LuAppUser6","request_time":"1564716723906","point_type":"event","category":"service","title":"list","action":"recharge","click":"1","is":" ","event_id":" ","platform":" ","ip":" ","lon":" ","lat":" ","ls":" ","product_id":" ","business_type":" ","business_id":" ","invest_id":" ","address":" ","type":" "}

     */
    private static String mokeJsonStringData (long timestamp) {
        String uuid = getUuid();
        String userIdAndUserName = getUserIdAndUserName(RANDOM.nextInt(10) + 1);
        String requestTime = getRequestTime(System.currentTimeMillis());
        String pointTypeAndCategory = getPointTypeAndCategory();
        String ts = "\"t_ms\":" + timestamp;
        return "{" +
                uuid + "," +
                userIdAndUserName + "," +
                requestTime + "," +
                pointTypeAndCategory + "," +
                "\"is\":" + "\" \"" + "," +
                "\"event_id\":" + "\" \"" + "," +
                "\"platform\":" + "\" \"" + "," +
                "\"ip\":" + "\" \"" + "," +
                "\"lon\":" + "\" \"" + "," +
                "\"lat\":" + "\" \"" + "," +
                "\"ls\":" + "\" \"" + "," +
                "\"product_id\":" + "\" \"" + "," +
                "\"business_type\":" + "\" \"" + "," +
                "\"business_id\":" + "\" \"" + "," +
                "\"invest_id\":" + "\" \"" + "," +
                "\"address\":" + "\" \"" + "," +
                "\"type\":" + "\" \"" +
                "}";
    }

    /*
            {"user_id":"1","user_name":"LuAppUser1","request_time":"1564716576497","point_type":"screen","category":"myAccount","title":"channel","click":"0"}
            {"user_id":"2","user_name":"LuAppUser2","request_time":"1564716576586","point_type":"screen","category":"recharge","title":"channel","click":"0"}
     */
    private static String mokeJsonStringSimpleData (long timestamp) {
        String userIdAndName = getUserIdAndUserName(RANDOM.nextInt(10) + 1);
        String requestTime = getRequestTime(System.currentTimeMillis());
        String pointTypeAndCategory = getPointTypeAndCategory();
        return "{" +
                userIdAndName + "," +
                requestTime + "," +
                pointTypeAndCategory +
                "}";
    }

    /*
            {"user_id":"2","user_name":"LuAppUser2","point_type":"screen","category":"myAccount","title":"message","click":"0"}
            {"user_id":"5","user_name":"LuAppUser5","point_type":"event","category":"home","title":"list","action":"recharge","click":"1"}
     */
    private static String mokeJsonStringDataWithoutTimestamp () {
        String userIdAndName = getUserIdAndUserName(RANDOM.nextInt(10) + 1);
        String pointTypeAndCategory = getPointTypeAndCategory();
        return "{" +
                userIdAndName + "," +
                pointTypeAndCategory +
                "}";
    }

    public static String mokeData(String param) {
        switch (param) {
            case "sqlProcessor":
                return mokeJsonStringData(System.currentTimeMillis());
            case "tableProcessor":
                return mokeJsonStringSimpleData(System.currentTimeMillis());
            default:
                return mokeJsonStringDataWithoutTimestamp();
        }
    }

    public static void main(String[] args) {
        for (int i = 0; i < 5; i++) {
            System.out.println(mokeData("sqlProcessor")
            );
        }
    }

}


