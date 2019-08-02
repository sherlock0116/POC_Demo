package com.lufax.mis.utils;

import com.lufax.mis.conf.ConfigurationManager;
import com.lufax.mis.constant.Constants;

import java.util.ArrayList;

/**
 * Description: ${DESCRIPTION}
 *
 * @author sherlock
 * @create 2019/5/9
 * @since 1.0.0
 */
public class WindowTimestampDesigner {

    private static ArrayList<Long> tsList;

    static {
        tsList = new ArrayList<Long>();
        long time_20190508140000 = ConfigurationManager.getLongPro(Constants.TIME_20190508140000);
        long time_20190508141500 = ConfigurationManager.getLongPro(Constants.TIME_20190508141500);
        long time_20190508143000 = ConfigurationManager.getLongPro(Constants.TIME_20190508143000);
        long time_20190508144500 = ConfigurationManager.getLongPro(Constants.TIME_20190508144500);
        long time_20190508150000 = ConfigurationManager.getLongPro(Constants.TIME_20190508150000);
        long time_20190508151500 = ConfigurationManager.getLongPro(Constants.TIME_20190508151500);
        long time_20190508153000 = ConfigurationManager.getLongPro(Constants.TIME_20190508153000);
        long time_20190508154500 = ConfigurationManager.getLongPro(Constants.TIME_20190508154500);
        long time_20190508160000 = ConfigurationManager.getLongPro(Constants.TIME_20190508160000);
        long time_20190508161500 = ConfigurationManager.getLongPro(Constants.TIME_20190508161500);
        long time_20190508163000 = ConfigurationManager.getLongPro(Constants.TIME_20190508163000);
        long time_20190508164500 = ConfigurationManager.getLongPro(Constants.TIME_20190508164500);
        long time_20190508170000 = ConfigurationManager.getLongPro(Constants.TIME_20190508170000);
        tsList.add(time_20190508140000);
        tsList.add(time_20190508141500);
        tsList.add(time_20190508143000);
        tsList.add(time_20190508144500);
        tsList.add(time_20190508150000);
        tsList.add(time_20190508151500);
        tsList.add(time_20190508153000);
        tsList.add(time_20190508154500);
        tsList.add(time_20190508160000);
        tsList.add(time_20190508161500);
        tsList.add(time_20190508163000);
        tsList.add(time_20190508164500);
        tsList.add(time_20190508170000);
    }

    public static ArrayList<Long> getTsList(){
        return tsList;
    }

    public static long getTimestamp(int index){
        if (index >= 0 && index < tsList.size()) {
            return tsList.get(index);
        }
        return -1;
    }


}
