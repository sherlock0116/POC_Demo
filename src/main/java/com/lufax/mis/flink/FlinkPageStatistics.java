package com.lufax.mis.flink;

import com.lufax.mis.conf.ConfigurationManager;
import com.lufax.mis.constant.Constants;
import com.lufax.mis.domain.PageViewCount;
import com.lufax.mis.utils.JsonUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
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
 * @create 2019/4/21
 * @since 1.0.0
 */
public class FlinkPageStatistics {

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

        /*
            create FlinkKafkaConsumer Source
         */
        Properties properties = initKafkaConfig();
        FlinkKafkaConsumer010<String> flinkKafkaConsumer010 = new FlinkKafkaConsumer010<>(Arrays.asList(TOPIC_NAME), new SimpleStringSchema(), properties);
        flinkKafkaConsumer010.setStartFromLatest();

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

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        long timeSize = parameterTool.getLong("timeSize", 1);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

        /*
         * SourceStream DataStructure:
         *      {"un":22,  "action":myAccount, "point_type":event,  "click":1, "title":message, "t_ms":1555924938442}
         *      {"un":233, "category":find,    "point_type":"screen", "click":0, "title":" ",    "t_ms":1555924938440}
         */
        DataStreamSource<String> sourceStream = env.addSource(flinkKafkaConsumer010);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 将Json的 value 封装到 Row 中
        DataStream<Row> rowStream = sourceStream.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String input) throws Exception {
                String _input = JsonUtils.subJsonString(input);
                Row row = new Row(6);
                String[] strings = _input.split(",");
                for (int i = 0; i < strings.length; i++) {
                    String value = strings[i].split(":")[1];
                    row.setField(i, value);
                }
                return row;
            }
        }).filter(row -> {
            return row.getArity() == 6;
        });


        DataStream<Row> _rowStream = rowStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Row>() {
            @Override
            public long extractAscendingTimestamp(Row row) {
                String _timestamp = (String) row.getField(5);
                long timestamp = Long.valueOf(_timestamp);
                return timestamp;
            }
        });

        /*
         *   SplitStream<Row> DataStructure:
         *      un      page       title     point_type   click    t_ms
         *      206,    myAccount, null,     screen,      0,       1556019567957
         *      184,    find,      bannel,   event,       1,       1556019567958
         */
        SplitStream<Row> _splitStream = _rowStream.split(row -> {
            ArrayList<String> outputs = new ArrayList<>();
            String point_type = (String)row.getField(3);
            if (point_type != null){
                if (point_type.equals("screen")){
                    outputs.add("screen");
                }else {
                    outputs.add("event");
                }
                return outputs;
            }
            return null;
        });


        KeyedStream<Row, String> keyedStream = _splitStream.select("screen")
                .keyBy(row -> {
                    return (String) (row.getField(1));
                });

        WindowedStream<Row, String, TimeWindow> windowedStream = keyedStream
                .timeWindow(Time.seconds(5));

        /*
           Screen UV Result:
             2> [PageViewCount{pageName='home',      windowEnd=2019-04-26 13:22:20, count=14},
                 PageViewCount{pageName='service',   windowEnd=2019-04-26 13:22:20, count=13},
                 PageViewCount{pageName='myAccount', windowEnd=2019-04-26 13:22:20, count=13},
                 PageViewCount{pageName='find',      windowEnd=2019-04-26 13:22:20, count=7}]

             2> [PageViewCount{pageName='service',   windowEnd=2019-04-26 13:22:25, count=26},
                 PageViewCount{pageName='home',      windowEnd=2019-04-26 13:22:25, count=25},
                 PageViewCount{pageName='myAccount', windowEnd=2019-04-26 13:22:25, count=25},
                 PageViewCount{pageName='find',      windowEnd=2019-04-26 13:22:25, count=20}]

             1> [PageViewCount{pageName='find',      windowEnd=2019-04-26 13:22:30, count=30},
                 PageViewCount{pageName='myAccount', windowEnd=2019-04-26 13:22:30, count=28},
                 PageViewCount{pageName='home',      windowEnd=2019-04-26 13:22:30, count=27},
                 PageViewCount{pageName='service',   windowEnd=2019-04-26 13:22:30, count=22}]

             3> [PageViewCount{pageName='find',      windowEnd=2019-04-26 13:22:35, count=29},
                 PageViewCount{pageName='service',   windowEnd=2019-04-26 13:22:35, count=26},
                 PageViewCount{pageName='myAccount', windowEnd=2019-04-26 13:22:35, count=20},
                 PageViewCount{pageName='home',      windowEnd=2019-04-26 13:22:35, count=16}]

         */
        windowedStream
                .evictor(new Evictor<Row, TimeWindow>() {
                    @Override
                    public void evictBefore(Iterable<TimestampedValue<Row>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {

                    }

                    @Override
                    public void evictAfter(Iterable<TimestampedValue<Row>> iterable, int i, TimeWindow timeWindow, EvictorContext evictorContext) {

                    }
                })
                .apply(new UvWindowStateFunction())
                .keyBy(uniquePageCount -> {
                    return uniquePageCount.getWindowEnd();
                })
                .process(new ScreenUVStatistics())
                .print();


        /*
           Screen PV Result:
            3> [PageViewCount{pageName='home',      windowEnd=2019-04-25 15:15:05, count=13},
                PageViewCount{pageName='service',   windowEnd=2019-04-25 15:15:05, count=12},
                PageViewCount{pageName='myAccount', windowEnd=2019-04-25 15:15:05, count=9 },
                PageViewCount{pageName='find',      windowEnd=2019-04-25 15:15:05, count=7}]

            2> [PageViewCount{pageName='service',   windowEnd=2019-04-25 15:15:10, count=29},
                PageViewCount{pageName='find',      windowEnd=2019-04-25 15:15:10, count=26},
                PageViewCount{pageName='home',      windowEnd=2019-04-25 15:15:10, count=22},
                PageViewCount{pageName='myAccount', windowEnd=2019-04-25 15:15:10, count=22}]

            4> [PageViewCount{pageName='myAccount', windowEnd=2019-04-25 15:15:15, count=34},
                PageViewCount{pageName='find',      windowEnd=2019-04-25 15:15:15, count=30},
                PageViewCount{pageName='home',      windowEnd=2019-04-25 15:15:15, count=20},
                PageViewCount{pageName='service',   windowEnd=2019-04-25 15:15:15, count=18}]

            4> [PageViewCount{pageName='home',      windowEnd=2019-04-25 15:15:20, count=27},
                PageViewCount{pageName='myAccount', windowEnd=2019-04-25 15:15:20, count=27},
                PageViewCount{pageName='service',   windowEnd=2019-04-25 15:15:20, count=26},
                PageViewCount{pageName='find',      windowEnd=2019-04-25 15:15:20, count=24}]
         */
//        windowedStream
//                .aggregate(new PvCountAccumulator(), new PvWindowStateFunction())
//                .keyBy(pageViewCount -> {
//                    return pageViewCount.getWindowEnd();
//                })
//                .process(new ScreenStatistics())
//                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}

/**
 * 窗口内un去重
 */
class UvWindowStateFunction implements WindowFunction<Row, PageViewCount, String, TimeWindow>{

    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Row> iterable, Collector<PageViewCount> collector) throws Exception {
        String pageName = key;
        HashMap<String, Row> rowHashMap = new HashMap<>();
        for (Row row : iterable) {
            String userName = (String) row.getField(0);
            rowHashMap.put(userName, row);
        }
        Set<String> userNames = rowHashMap.keySet();
        long count = userNames.size();
        long windowEnd = timeWindow.getEnd();
        collector.collect(new PageViewCount(pageName, windowEnd, count));
    }
}

/**
 * 按窗口结束时间分组, 统计组内所有PageViewCount的page的UV
 */
class ScreenUVStatistics extends KeyedProcessFunction<Long, PageViewCount, List<PageViewCount>>{

    private ListState<PageViewCount> uvState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        uvState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("uvState", PageViewCount.class));
    }

    @Override
    public void processElement(PageViewCount pageViewCount, Context context, Collector<List<PageViewCount>> collector) throws Exception {
        uvState.add(pageViewCount);
        context.timerService().registerEventTimeTimer(pageViewCount.getWindowEnd());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<PageViewCount>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        ArrayList<PageViewCount> pageViewCounts = new ArrayList<>();
        for (PageViewCount pageViewCount : uvState.get()) {
            pageViewCounts.add(pageViewCount);
        }
        uvState.clear();
        pageViewCounts.sort(new Comparator<PageViewCount>() {
            @Override
            public int compare(PageViewCount o1, PageViewCount o2) {
                return (int) (o2.getCount() - o1.getCount());
            }
        });
        out.collect(pageViewCounts);
    }
}

/**
 *  窗口内计数器,计算页面PV
 */
class PvCountAccumulator implements AggregateFunction<Row, Long, Long>{

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Row row, Long acc) {
        return acc + 1L;
    }

    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    @Override
    public Long merge(Long acc2, Long acc1) {
        return acc2 + acc1;
    }
}

/**
  * 窗口状态统计函数,返回出PageViewCount
  */
class PvWindowStateFunction implements WindowFunction<Long, PageViewCount, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Long> counts, Collector<PageViewCount> collector) throws Exception {
//        String _key = ((Tuple1<String>) key).f0;
        PageViewCount pageViewCount = new PageViewCount(key, timeWindow.getEnd(), counts.iterator().next());
        collector.collect(pageViewCount);
    }
}

/**
  * 按窗口结束时间分组后,统计组内所有PageViewCount的page的PV/UV
  */
class ScreenStatistics extends KeyedProcessFunction<Long, PageViewCount, List<PageViewCount>>{

    private ListState<PageViewCount> statisticsState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 注册ListState
        statisticsState = getRuntimeContext().getListState(new ListStateDescriptor<PageViewCount>("statisticsState", PageViewCount.class));
    }

    @Override
    public void processElement(PageViewCount input, Context context, Collector<List<PageViewCount>> collector) throws Exception {
        // 添加输入到ListState
        statisticsState.add(input);
        // 注册事件,水印到达windowEnd+1,触发onTimer
        context.timerService().registerEventTimeTimer(input.getWindowEnd());
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<PageViewCount>> out) throws Exception {
        super.onTimer(timestamp, ctx, out);
        ArrayList<PageViewCount> pageViewCounts = new ArrayList<PageViewCount>();
        for (PageViewCount pageViewCount : statisticsState.get()) {
            pageViewCounts.add(pageViewCount);
        }
        statisticsState.clear();
        pageViewCounts.sort(new Comparator<PageViewCount>() {
            @Override
            public int compare(PageViewCount o1, PageViewCount o2) {
                return (int) (o2.getCount() - o1.getCount());
            }
        });
        out.collect(pageViewCounts);
    }


}




