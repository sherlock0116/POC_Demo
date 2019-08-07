package com.lufax.mis.flink;

import com.google.common.collect.Maps;
import com.lufax.mis.constant.Constants;
import com.lufax.mis.kafka.KafkaTopics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.descriptors.StreamTableDescriptor;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Description: ${DESCRIPTION}
 *
 * @author sherlock
 * @create 2019/5/17
 * @since 1.0.0
 */
public class FlinkTableProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkTableProcessor.class);

    public static void main(String[] args) {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment streamTableEnv = TableEnvironment.getTableEnvironment(streamEnv);

        /* kafka consumer properties */
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, Constants.KAFKA_LOCAL_GROUP_ID);
        // TODO
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "");

        StreamTableDescriptor tableDescriptor = streamTableEnv.connect(
                new Kafka()
                        .version("0.10")
                        .topic(KafkaTopics.getSourceTopic())
                        .startFromLatest()
                        .properties(properties)
        )
                .withFormat(
                        new Json()
                                /* 当字段缺失的时候是否解析失败(默认false) */
                                .failOnMissingField(false)
                                /* [方式1] 使用Flink数据类型定义. 然后通过mapping映射成JSON Schema */
//                                .schema(Types.ROW())
                                /* [方式2] 通过配置jsonSchema构建 JSON FORMAT */
//                                .jsonSchema(
//                                        "{" +
//                                                "type: 'object'," +
//                                                "properties: {" +
//                                                    "user_id: {type: 'string'}," +
//                                                    "user_name: {type: 'string'}," +
//                                                    "request_time: {type:'string',format: 'date-time'}," +
//                                                    "point_type: {type: 'string'}," +
//                                                    "category: {type: 'string'}," +
//                                                    "title: {type: string}," +
//                                                    "action: {type: string}" +
//                                                "}" +
//                                        "}"
//                                )
                                /* [方式3] 直接使用Table中的Schema信息,转换成JSON结构 */
                                .deriveSchema()
                );

        /* kafka producer 每秒产生一条数据, 基于处理时间的数据流,一个10s的滑动窗口, 统计每个窗口内处理的数据量是否为10 */
        String sql1 =
                "select " +
                        "count(point_type), " +
                        "TIMESTAMPADD(HOUR, 8, TUMBLE_START(sysProctime, INTERVAL '10' SECOND)) AS winStart " +
                "from insideMap1 " +
                "group by TUMBLE(sysProctime, INTERVAL '10' SECOND)";

        String sql2 =
                "select " +
                        "user_name, " +
                        "count(point_type), " +
                        "count(title), " +
                        "TIMESTAMPADD(HOUR, 8, TUMBLE_START(sysProctime, INTERVAL '10' SECOND)) AS winStart " +
                "from insideMap2 " +
                "group by (user_name, TUMBLE(sysProctime, INTERVAL '10' SECOND)) " +
                "having user_name = 'LuAppUser8'";

        HashMap<String, HashMap<String, String>> map = Maps.newHashMap();

        HashMap<String, String> insideMap1 = Maps.newHashMap();
        insideMap1.put("fields", "user_id,user_name,request_time,point_type,category,title,action");
        insideMap1.put("tableName", "insideMap1");
        insideMap1.put("sql", sql1);
        insideMap1.put("features", "");	// TODO 用户传入的特征参数
        map.put("k1", insideMap1);

        HashMap<String, String> insideMap2 = Maps.newHashMap();
        insideMap2.put("fields", "user_id,user_name,request_time,point_type,title");
        insideMap2.put("tableName", "insideMap2");
        insideMap2.put("sql", sql2);
        insideMap2.put("features", "");	// TODO 用户传入的特征参数
        map.put("k2", insideMap2);

        for (Map.Entry<String, HashMap<String, String>> entry : map.entrySet()) {
            String[] fields = entry.getValue().get("fields").split(",");
            String tableName = entry.getValue().get("tableName");
            String sql = entry.getValue().get("sql");
            Schema schema = new Schema();
            for (String field : fields) {
                schema.field(field, Types.STRING());
            }
            schema.field("sysProctime", Types.SQL_TIMESTAMP()).proctime();
            tableDescriptor.withSchema(schema)
                    .inAppendMode()
                    .registerTableSource(tableName);
            Table table = streamTableEnv.sqlQuery(sql);
            DataStream<Row> rowDataStream = streamTableEnv.toAppendStream(table, Types.ROW());
            rowDataStream.print();
        }

        try {
            streamTableEnv.execEnv();
            streamEnv.execute();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
