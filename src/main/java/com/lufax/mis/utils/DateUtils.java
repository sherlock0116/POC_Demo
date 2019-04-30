package com.lufax.mis.utils;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Description: ${DESCRIPTION}
 *
 * @author sherlock
 * @create 2019/4/19
 * @since 1.0.0
 */
public class DateUtils {

    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private static final DateFormat TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final Date DATE = new Date();

    public static String getCurrentDate(){
        String currentDate = DATE_FORMAT.format(DATE);
        return currentDate;
    }

    public static String getTimestamp2DateStr(long timeStamp){
        Date date = new Date(timeStamp);
        return TIME_FORMAT.format(date);
    }

}

class Test{

    public static void main(String[] args) throws Exception{

        String currentDate = DateUtils.getCurrentDate();
        System.out.println("currentDate: " + currentDate);


        String dateStr = DateUtils.getTimestamp2DateStr(1556083897968L);
        System.out.println(dateStr);


    }
}

