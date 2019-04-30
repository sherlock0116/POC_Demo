package com.lufax.mis;

import com.lufax.mis.conf.ConfigurationManager;
import com.lufax.mis.constant.Constants;
import com.lufax.mis.domain.DataGenerator;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * kafka消息生产者
 *
 * @author sherlock
 * @create 2019/4/19
 * @since 1.0.0
 */
public class KafkaMsgProducer {

    private static Logger logger = LoggerFactory.getLogger(KafkaMsgProducer.class.getName());

    private static final String BOOTSTRAP_SERVERS = ConfigurationManager.getProperty(Constants.KAFKA_LOCAL_BOOTSTRAP_SERVERS);
    private static final String TOPIC_NAME = ConfigurationManager.getProperty(Constants.KAFKA_LOCAL_TOPIC_NAME);
    private static final AtomicBoolean IS_FLAG = new AtomicBoolean(true);

    public static Properties initConfig(){
        Properties propers = new Properties();
        propers.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        propers.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propers.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        return propers;
    }

    public static void main(String[] args) throws Exception{
        Properties propers = initConfig();
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(propers);
        while (IS_FLAG.get()){
            kafkaProducer.send(new ProducerRecord<String, String>(TOPIC_NAME, null, DataGenerator.mokeData()), new Callback() {

                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null){
                        logger.error("ERROR:{}", e);
                    }
                }
            });
            Thread.sleep(20);
        }

    }
}
