package com.lufax.mis;

import com.lufax.mis.kafka.KafkaHelper;
import com.lufax.mis.kafka.KafkaTopics;
import com.lufax.mis.utils.DateUtils;
import com.lufax.mis.utils.WindowTimestampDesigner;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * kafka消息生产者
 *
 * @author sherlock
 * @create 2019/4/19
 * @since 1.0.0
 */
public class KafkaMsgProducer {

    private static Logger log = LoggerFactory.getLogger(KafkaMsgProducer.class.getName());
    private static final AtomicBoolean IS_FLAG = new AtomicBoolean(true);

    public static void main(String[] args) throws Exception{

        String sourceTopic = KafkaTopics.getSourceTopic();
        String driverTopic = KafkaTopics.getDriverTopic();

        ExecutorService threadPool = Executors.newFixedThreadPool(3);

        Properties propers = KafkaHelper.getKafkaProducerProperty();
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(propers);

        long startTime = System.currentTimeMillis();
        log.info("=====> 开始的发送数据 <=====: " + DateUtils.getTimestamp2DateStr(startTime));

        /* 线程1: 数据写入sourceTopic */
        threadPool.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    while (IS_FLAG.get()) {
                        String jsonStringData = DataGenerator.mokeData("tableProcessor");
                        System.out.println(jsonStringData);     // 打印源数据
                        kafkaProducer.send(new ProducerRecord<String, String>(sourceTopic, null, jsonStringData));
                        Thread.sleep(1000);
                    }
                }catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    threadPool.shutdown();
                }
            }
        });

        /* 线程2: 数据写入driverTopic */
//        threadPool.submit(new Runnable() {
//            @Override
//            public void run() {
//                try {
//                    String msg1 = "{" +
//                            "\"fields\":\"userId,userName,requestTime,pointType,category,title,action\"" +
//                            "\"types\":\"long,string,long,string,string,string,string\"" +
//                            "\"tableName\":\"msg1\"" +
//                            "\"features\":\"feature1,feature2\"" +
//                            "\"sql\":\"select count() from \"";
//                    kafkaProducer.send(new ProducerRecord<>(driverTopic, null, msg1));
//                    Thread.sleep(500);
//                }catch (Exception e) {
//                    e.printStackTrace();
//                }finally {
//                    threadPool.shutdown();
//                }
//            }
//        });

//        while (IS_FLAG.get()){
//        for (int i = 0; i < 1000000; i++) {
//            kafkaProducer.send(new ProducerRecord<String, String>(sourceTopic, null, DataGenerator.mokeRealTimeData(System.currentTimeMillis())), new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                    if (e != null){
//                        log.error("ERROR:{}", e);
//                    }
//                }
//            });
//            Thread.sleep(1);
//        }

        /* 每小时写入100万数据,测试Flink处理速度 */
//        ArrayList<Long> tsList = WindowTimestampDesigner.getTsList();
//        for (long ts: tsList) {
//            for (int i = 0; i < 1000000; i++) {
//                kafkaProducer.send(new ProducerRecord<String, String>(sourceTopic, null, DataGenerator.mokeJsonStringData(ts)), new Callback() {
//                    @Override
//                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//                        if (e != null) {
//                            log.error("ERROR: {}", e);
//                        }
//                    }
//                });
//            }
//        }

        long endTime = System.currentTimeMillis();
        log.info("=====> 结束发送数据 <===== : " + DateUtils.getTimestamp2DateStr(endTime));
        double timeUsed = (double)((endTime - startTime)/1000);
        log.info("用时: " + timeUsed);

        kafkaProducer.close();
    }
}
