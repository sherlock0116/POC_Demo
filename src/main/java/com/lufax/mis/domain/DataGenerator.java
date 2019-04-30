package com.lufax.mis.domain;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import com.lufax.mis.utils.DateUtils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 模拟数据的生成类
 *
 * @author sherlock
 * @create 2019/4/19
 * @since 1.0.0
 */
public class DataGenerator {

    static Random random = new Random();

    static String getUn(){
        int _un = random.nextInt(2) + 1;
        String un = String.valueOf(_un);
//        return "\"un\":" + "\"" + un + "\"";
        return "\"un\":" + un;
    }

    static String getTitle(){
        List<String> titles = Arrays.asList("channel", "message", "list", "bannel");
        String _title = titles.get(random.nextInt(4));
//        return "\"title\":" + "\"" + _title + "\"";
        return "\"title\":" + _title;
    }

    static String getPointTypeAndCategory(){
        List<String> pointTypes = Arrays.asList("screen", "event");
        List<String> categories = Arrays.asList("home", "service", "find", "myAccount");
        List<String> actions = Arrays.asList("home", "service", "find", "myAccount");
        String _pointType = pointTypes.get(random.nextInt(2));
        String _category = categories.get(random.nextInt(4));
        String _action = actions.get(random.nextInt(4));
        if (_pointType != null){
            if (_pointType == "event"){
//                return "\"action\":" +  "\"" + _action + "\"" + "," + getTitle() + "," + "\"point_type\":" + "\"event\"" + "," + "\"click\":" + "\"" + 1 + "\"";
                return "\"action\":" + _action + "," + getTitle() + "," + "\"point_type\":" + "event" + "," + "\"click\":" + 1;
            }else {
//                return "\"category\":" + "\"" + _category + "\"" + "," + "\"title\":" + "\" \"" + "," + "\"point_type\":" + "\"screen\"" + "," + "\"click\":" + "\"" + 0 + "\"";
                return "\"category\":" + _category + "," + "\"title\":" + null + "," + "\"point_type\":" + "screen" + "," + "\"click\":" + 0;
            }
        }
        return null;
    }

    static String getTimestamp(){

        long _timestamp = System.currentTimeMillis();
        return "\"t_ms\":" + _timestamp;
    }

    public static String mokeData(){
        String un = getUn();
        String pointTypeAndCategory = getPointTypeAndCategory();
        String ts = getTimestamp();
        return "{" + un + "," + pointTypeAndCategory + "," + ts + "}";
    }
}

class Test{
    public static void main(String[] args) {

//        String jsonStr = "{\"name\":\"cat\",\"sex\":\"miao\"}";
//        String substring = jsonStr.substring(1,jsonStr.length()-1);
//        System.out.println(substring);
//
//        Date date = new Date(1556083897968L);
//        System.out.println(date);
//        DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
//        System.out.println(sdf.format(date));

//        while (true) {
//            System.out.println(DataGenerator.mokeData());
//        }

//        ArrayList<Integer> integers = new ArrayList<>();
//        integers.add(2);
//        integers.add(8);
//        integers.add(6);
//        integers.add(12);
//        integers.add(1);
//        integers.add(4);
//
//        integers.sort(new Comparator<Integer>() {
//            @Override
//            public int compare(Integer o1, Integer o2) {
//                return (o2 - o1);
//            }
//        });
//
//        System.out.println(integers.toString());

        HashSet<Integer> set = new HashSet<>();
        ArrayList<Integer> integers = new ArrayList<>();
        integers.add(1);
        integers.add(2);
        integers.add(3);
        integers.add(1);
        integers.add(2);
        integers.add(3);

        for (int num : integers) {
            set.add(num);
        }

        System.out.println(set.toString());

        HashMap<Integer, Integer> map = new HashMap<>();
        map.put(1,2);
        map.put(2,3);
        map.put(3,4);
        map.put(1,9);
        System.out.println(map.toString());


    }
}

