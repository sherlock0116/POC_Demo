package com.lufax.mis.flink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.Maps;
import com.lufax.mis.conf.ConfigurationManager;
import com.lufax.mis.constant.Constants;
import com.lufax.mis.domain.CalEventBus;
import com.lufax.mis.domain.PageViewCount;
import com.lufax.mis.domain.ScreenPageView;
import com.lufax.mis.elasticSearch.EsHelper;
import com.lufax.mis.kafka.KafkaHelper;
import com.lufax.mis.kafka.KafkaTopics;
import com.lufax.mis.utils.JsonUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;


/**
 * Description: ${DESCRIPTION}
 *
 * @author sherlock
 * @create 2019/4/26
 * @since 1.0.0
 */
public class FlinkSQLProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkPageStatistics.class.getName());

    public static void main(String[] args) {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnv = TableEnvironment.getTableEnvironment(streamEnv);

//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
//        BatchTableEnvironment batchTableEnv = TableEnvironment.getTableEnvironment(batchEnv);

        /* set args global */
        streamEnv.getConfig().setGlobalJobParameters(parameterTool);

        /* checkpoint config */
//        streamEnv.enableCheckpointing(5000);
//        streamEnv.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        streamEnv.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
//        streamEnv.getCheckpointConfig().setCheckpointTimeout(60000);
//        streamEnv.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        streamEnv.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /* create kafka consumer */
        Properties property = KafkaHelper.getKafkaConsumerProperty();
        String sourceTopic = KafkaTopics.getSourceTopic();
        FlinkKafkaConsumer010<String> flinkKafkaConsumer010 = new FlinkKafkaConsumer010<>(sourceTopic, new SimpleStringSchema(), property);

        /* set consumer offsets */
//        flinkKafkaConsumer010.setStartFromEarliest();
        flinkKafkaConsumer010.setStartFromLatest();
//        flinkKafkaConsumer010.setStartFromGroupOffsets();
//        flinkKafkaConsumer010.setStartFromTimestamp(1557308100000L);
//        HashMap<KafkaTopicPartition, Long> specificOffsets = Maps.newHashMap();
//        flinkKafkaConsumer010.setStartFromSpecificOffsets(specificOffsets);

        /* create Elasticsearch Sink */
        Map<String, String> esConfig = EsHelper.getEsSinkConfig();
        List<InetSocketAddress> esSocketAddress = EsHelper.getEsAddress();
        ElasticsearchSink<PageViewCount> elasticsearchSink = new ElasticsearchSink<>(esConfig, esSocketAddress, new ElasticsearchSinkFunction<PageViewCount>() {
            @Override
            public void process(PageViewCount pageViewCount, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                // TODO
            }
        });

        DataStreamSource<String> sourceStream = streamEnv.addSource(flinkKafkaConsumer010);

        DataStream<CalEventBus> pojoStream = sourceStream.map(input -> {
             return JSON.parseObject(input, new TypeReference<CalEventBus>() {});
        }).filter(calEventBus -> {
            return !StringUtils.isNullOrWhitespaceOnly(calEventBus.getUser_id());
        });

        streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<CalEventBus> calEventBusStream = pojoStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<CalEventBus>() {
            @Override
            public long extractAscendingTimestamp(CalEventBus calEventBus) {
                return calEventBus.getRequest_time();
            }
        });

        streamTableEnv.registerDataStream("cal_event_bus", calEventBusStream,
                "uuid, user_id, user_name, request_time.rowtime, point_type, category, title, action, click, is, event_id, platform, ip, lon, lat, ls, product_id, business_type, business_id, invest_id, address, type");

        String sql =
                "SELECT " +
                        "count(1), " +
                        "TIMESTAMPADD(HOUR, 8, TUMBLE_START(request_time, INTERVAL '5' SECOND)) AS winStart, " +
                        "TIMESTAMPADD(HOUR, 8, TUMBLE_END(request_time, INTERVAL '5' SECOND)) AS winEnd " +
                "FROM cal_event_bus " +
                "WHERE point_type = 'screen' AND action = 'recharge' " +
                "GROUP BY TUMBLE(userActionTime, INTERVAL '5' SECOND), pageName";


        Table relTable = streamTableEnv.sqlQuery(sql);
        DataStream<Row> relStream = streamTableEnv.toAppendStream(relTable, Row.class);
        relStream.print();

//        System.out.println(tEnv.explain(pv Table));

        try {
            streamTableEnv.execEnv();
            streamEnv.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
