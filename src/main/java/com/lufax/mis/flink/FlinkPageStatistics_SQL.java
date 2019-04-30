package com.lufax.mis.flink;

import com.lufax.mis.conf.ConfigurationManager;
import com.lufax.mis.constant.Constants;
import com.lufax.mis.domain.PageViewCount;
import com.lufax.mis.domain.ScreenPageView;
import com.lufax.mis.utils.JsonUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
public class FlinkPageStatistics_SQL {

    private static Logger logger = LoggerFactory.getLogger(FlinkPageStatistics.class.getName());

    // Kafka
    private static final String BOOTSTRAP_SERVERS = ConfigurationManager.getProperty(Constants.KAFKA_LOCAL_BOOTSTRAP_SERVERS);
    private static final String GROUP_ID = ConfigurationManager.getProperty(Constants.KAFKA_LOCAL_GROUP_ID);
    private static final String TOPIC_NAME = ConfigurationManager.getProperty(Constants.KAFKA_LOCAL_TOPIC_NAME);

    // ES
    private static final String ES_CLUSTER_NAME = ConfigurationManager.getProperty(Constants.ES_CLUSTER_NAME);
    private static final String ES_BULK_FLUSH_MAX_ACTIONS = ConfigurationManager.getProperty(Constants.ES_BULK_FLUSH_MAX_ACTIONS);
    private static final String ES_INETADDRESS_NAME_1 = ConfigurationManager.getProperty(Constants.ES_INETADDRESS_NAME_1);
    private static final String ES_INETADDRESS_NAME_2 = ConfigurationManager.getProperty(Constants.ES_INETADDRESS_NAME_2);
    private static final int ES_PORT = 9003;


    /*
        initialize Kafka config
     */
    private static Properties initKafkaConfig(){
        Properties propers = new Properties();
        propers.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        propers.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        return propers;
    }

    /*
        initialize ES config
     */
    private static Map<String, String> initESConfig(){
        HashMap<String, String> esConfig = new HashMap<>();
        esConfig.put(Constants.ES_CLUSTER_NAME_CONFIG, ES_CLUSTER_NAME);
        esConfig.put(Constants.ES_BULK_FLUSH_MAX_ACTIONS_CONFIG, ES_BULK_FLUSH_MAX_ACTIONS);
        return esConfig;
    }

    /*
        initialize ES address
     */
    private static List<InetSocketAddress> initESSocketAddress() {
        ArrayList<InetSocketAddress> addressArrayList = new ArrayList<>();
        try {
            addressArrayList.add(new InetSocketAddress(InetAddress.getByName(ES_INETADDRESS_NAME_1), ES_PORT));
            addressArrayList.add(new InetSocketAddress(InetAddress.getByName(ES_INETADDRESS_NAME_2), ES_PORT));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return  addressArrayList;
    }

    public static void main(String[] args) {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        long timeSize = parameterTool.getLong("timeSize", 1);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

        /*
            create Elasticsearch Sink
         */
        Map<String, String> esConfig = initESConfig();
        List<InetSocketAddress> esSocketAddress = initESSocketAddress();
        ElasticsearchSink<PageViewCount> elasticsearchSink = new ElasticsearchSink<>(esConfig, esSocketAddress, new ElasticsearchSinkFunction<PageViewCount>() {
            @Override
            public void process(PageViewCount pageViewCount, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {

            }
        });

        /*
            create FlinkKafkaConsumer Source
         */
        Properties properties = initKafkaConfig();
        FlinkKafkaConsumer010<String> flinkKafkaConsumer010 = new FlinkKafkaConsumer010<>(Arrays.asList(TOPIC_NAME), new SimpleStringSchema(), properties);
        flinkKafkaConsumer010.setStartFromLatest();
        DataStreamSource<String> sourceStream = env.addSource(flinkKafkaConsumer010);

        /*
         *   kafkaData<Json> DataStructure
         *      {un:206, screen:myAccount, title:null,   point_type:screen, click:0, t_ms:1556019567957}
         *      {un:184, screen:find,      title:bannel, point_type:event,  click:1, t_ms:1556019567958}
         */
        DataStream<ScreenPageView> pojoStream = sourceStream.map(new MapFunction<String, ScreenPageView>() {
            @Override
            public ScreenPageView map(String input) throws Exception {
                ScreenPageView screenPageView = new ScreenPageView();

                String _input = JsonUtils.subJsonString(input);
                String[] keyValues = _input.split(",");
                String userName = keyValues[0].split(":")[1];
                String pageName = keyValues[1].split(":")[1];
                String title = keyValues[2].split(":")[1];
                String pointType = keyValues[3].split(":")[1];
                int click = Integer.valueOf(keyValues[4].split(":")[1]);
                long userActionTime = Long.valueOf(keyValues[5].split(":")[1]);
//                Timestamp userActionTime = new Timestamp(_userActionTime);

                screenPageView.setUserName(userName);
                screenPageView.setPageName(pageName);
                screenPageView.setPointType(pointType);
                screenPageView.setClick(click);
                screenPageView.setTitle(title);
                screenPageView.setUserActionTime(userActionTime);

                return screenPageView;
            }
        });


        /*
         *   pojoStream<ScreenPageView> DataStructure:
         *      un      page       title     point_type   click    t_ms
         *      206,    myAccount, null,     screen,      0,       1556019567957
         *      184,    find,      bannel,   event,       1,       1556019567958
         */
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<ScreenPageView> _pojoStream = pojoStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ScreenPageView>() {
            @Override
            public long extractAscendingTimestamp(ScreenPageView screenPageView) {
                return screenPageView.getUserActionTime();
            }
        });

        tEnv.registerDataStream("pojoStream", _pojoStream, "userName, pageName, pointType, click, title, userActionTime.rowtime");

        String pvSQL =
                "SELECT " +
                        "pageName, count(*), max(click), TUMBLE_START(userActionTime, INTERVAL '5' SECOND) AS wStart " +
                "FROM  " +
                        "pojoStream " +
                "WHERE " +
                        "click = 0 " +
                "GROUP BY " +
                        "TUMBLE(userActionTime, INTERVAL '5' SECOND), pageName";

        /*
          UV QueryResult:
            2> service,   2,  0,  2019-04-29 01:10:15.0
            1> home,      2,  0,  2019-04-29 01:10:15.0
            2> myAccount, 2,  0,  2019-04-29 01:10:15.0
            2> find,      2,  0,  2019-04-29 01:10:15.0
            2> myAccount, 2,  0,  2019-04-29 01:10:20.0
            1> home,      2,  0,  2019-04-29 01:10:20.0
            2> find,      2,  0,  2019-04-29 01:10:20.0
            2> service,   2,  0,  2019-04-29 01:10:20.0
        */
        String uvSQL =
                "SELECT " +
                        "pageName, count(distinct userName), max(click), TUMBLE_START(userActionTime, INTERVAL '5' SECOND) AS wStart " +
                "FROM " +
                        "pojoStream " +
                "WHERE " +
                        "click = 0 " +
                "GROUP BY " +
                        "TUMBLE(userActionTime, INTERVAL '5' SECOND), pageName";

//        Table pvTable = tEnv.sqlQuery(pvSQL);
//        DataStream<Row> appendStream = tEnv.toAppendStream(pvTable, Row.class);

        Table uvTable = tEnv.sqlQuery(uvSQL);
        DataStream<Row> appendStream = tEnv.toAppendStream(uvTable, Row.class);

        appendStream.print();
//        System.out.println(tEnv.explain(pVTable));

        try {
            tEnv.execEnv();
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
