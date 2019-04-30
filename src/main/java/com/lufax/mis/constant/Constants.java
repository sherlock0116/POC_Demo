package com.lufax.mis.constant;

/**
 * 常量接口
 *
 * @author sherlock
 * @create 2019/4/22
 * @since 1.0.0
 */
public interface Constants {

    /*
     * local_kafka configuration value
     */
    String KAFKA_LOCAL_BOOTSTRAP_SERVERS= "kafka.local.bootstrap.servers";
    String KAFKA_LOCAL_TOPIC_NAME = "kafka.local.topic.name";
    String KAFKA_LOCAL_GROUP_ID = "kafka.local.group.id";

    /*
     * cluster_kafka configuration value
     */
    String KAFKA_CLUSTER_BOOTSTRAP_SERVERS = "kafka.cluster.bootstrap.servers";
    String KAFKA_CLUSTER_TOPIC_NAME = "kafka.cluster.topic.name";
    String KAFKA_CLUSTER_GROUP_ID = "kafka.cluster.group.id";

    /*
     * ES configuration key
     */
    String ES_CLUSTER_NAME_CONFIG = "cluster.name";
    String ES_BULK_FLUSH_MAX_ACTIONS_CONFIG = "bulk.flush.max.actions";

    /*
     * ES configuration value
     */
    String ES_CLUSTER_NAME = "es.cluster.name";
    String ES_BULK_FLUSH_MAX_ACTIONS = "es.bulk.flush.max.actions";
    String ES_INETADDRESS_NAME_1 = "es.inetAddress.name.1";
    String ES_INETADDRESS_NAME_2 = "es.inetAddress.name.2";



}
