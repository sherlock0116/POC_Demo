package com.lufax.mis.kafka;

import com.lufax.mis.conf.ConfigurationManager;
import com.lufax.mis.constant.Constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.tools.nsc.transform.patmat.ScalaLogic;

import java.util.Properties;

/**
 * Description: ${DESCRIPTION}
 *
 * @author sherlock
 * @create 2019/5/7
 * @since 1.0.0
 */
public class KafkaHelper {

    private static final boolean IS_LOCAL = Constants.IS_LOCAL;
    private static Properties propers = null;

    /**
     * 根据是否为生产环境,初始化KafkaProducerConfig
     * @return Properties
     */
    public static Properties getKafkaProducerProperty() {
        propers = new Properties();
        if (IS_LOCAL) {
            propers.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_LOCAL_BOOTSTRAP_SERVERS);
            propers.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            propers.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        } else {
            propers.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
            propers.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            propers.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }
        return propers;
    }

    /**
     * 根据是否为生产环境,初始化KafkaConsumerConfig
     * @return Properties
     */
    public static Properties getKafkaConsumerProperty() {
        propers = new Properties();
        if (IS_LOCAL) {
            propers.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_LOCAL_BOOTSTRAP_SERVERS);
            propers.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.KAFKA_LOCAL_GROUP_ID);
        } else {
            propers.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_CLUSTER_BOOTSTRAP_SERVERS);
            propers.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.KAFKA_CLUSTER_GROUP_ID);
        }
        return propers;
    }

}
