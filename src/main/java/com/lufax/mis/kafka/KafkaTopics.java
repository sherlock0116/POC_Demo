package com.lufax.mis.kafka;

import com.lufax.mis.constant.Constants;
import scala.tools.nsc.transform.patmat.ScalaLogic;

/**
 * Descriptor:
 * Author: sherlock
 * Date: 2019-08-02 11:18 AM
 */
public class KafkaTopics {

    private static String sourceTopic;
    private static String driverTopic;

    public static String getSourceTopic () {
        if (Constants.IS_LOCAL) {
            sourceTopic = Constants.KAFKA_LOCAL_SOURCE_TOPIC_NAME;
        }else {
            sourceTopic = Constants.KAFKA_CLUSTER_SOURCE_TOPIC_NAME;
        }
        return sourceTopic;
    }

    public static String getDriverTopic () {
        if (Constants.IS_LOCAL) {
            driverTopic = Constants.KAFKA_LOCAL_DRIVER_TOPIC_NAME;
        }else {
            driverTopic = Constants.KAFKA_CLUSTER_DRIVER_TOPIC_NAME;
        }
        return driverTopic;
    }
}
