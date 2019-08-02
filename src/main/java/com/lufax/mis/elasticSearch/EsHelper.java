package com.lufax.mis.elasticSearch;

import com.lufax.mis.conf.ConfigurationManager;
import com.lufax.mis.constant.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Description: ${DESCRIPTION}
 *
 * @author sherlock
 * @create 2019/5/13
 * @since 1.0.0
 */
public class EsHelper {
    private static Logger log = LoggerFactory.getLogger("elasticSearch");

    // ES
    private static final String ES_CLUSTER_NAME = ConfigurationManager.getProperty(Constants.ES_CLUSTER_NAME);
    private static final String ES_BULK_FLUSH_MAX_ACTIONS = ConfigurationManager.getProperty(Constants.ES_BULK_FLUSH_MAX_ACTIONS);
    private static final String ES_INETADDRESS_NAME_1 = ConfigurationManager.getProperty(Constants.ES_INETADDRESS_NAME_1);
    private static final String ES_INETADDRESS_NAME_2 = ConfigurationManager.getProperty(Constants.ES_INETADDRESS_NAME_2);
    private static final int ES_PORT = 9003;

    private static Map<String, String> esConfig = null;
    private static List<InetSocketAddress> addressList = null;

    public static Map<String, String> getEsSinkConfig(){
        esConfig = new HashMap<>();
        esConfig.put(Constants.ES_CLUSTER_NAME_CONFIG, ES_CLUSTER_NAME);
        esConfig.put(Constants.ES_BULK_FLUSH_MAX_ACTIONS_CONFIG, ES_BULK_FLUSH_MAX_ACTIONS);
        return esConfig;
    }

    public static List<InetSocketAddress> getEsAddress(){
        addressList = new ArrayList<>();
        try {
            addressList.add(new InetSocketAddress(InetAddress.getByName(ES_INETADDRESS_NAME_1), ES_PORT));
            addressList.add(new InetSocketAddress(InetAddress.getByName(ES_INETADDRESS_NAME_2), ES_PORT));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return  addressList;
    }

}
