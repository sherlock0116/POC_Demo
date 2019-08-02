package com.lufax.mis.constant;

import com.lufax.mis.conf.ConfigurationManager;

/**
 * 常量接口
 *
 * @author sherlock
 * @create 2019/4/22
 * @since 1.0.0
 */
public interface Constants {

    boolean IS_LOCAL = ConfigurationManager.getBooleanPro("is.local");

    /**
      *  =====>  kafka configuration  <=====
      */
    String KAFKA_LOCAL_BOOTSTRAP_SERVERS = ConfigurationManager.getProperty("kafka.local.bootstrap.servers");
    String KAFKA_LOCAL_SOURCE_TOPIC_NAME = ConfigurationManager.getProperty("kafka.local.source.topic.name");
    String KAFKA_LOCAL_DRIVER_TOPIC_NAME = ConfigurationManager.getProperty("kafka.local.driver.topic.name");
    String KAFKA_LOCAL_GROUP_ID = ConfigurationManager.getProperty("kafka.local.group.id");

    String KAFKA_CLUSTER_BOOTSTRAP_SERVERS = ConfigurationManager.getProperty("kafka.cluster.bootstrap.servers");
    String KAFKA_CLUSTER_SOURCE_TOPIC_NAME = ConfigurationManager.getProperty("kafka.cluster.source.topic.name");
    String KAFKA_CLUSTER_DRIVER_TOPIC_NAME = ConfigurationManager.getProperty("kafka.cluster.driver.topic.name");
    String KAFKA_CLUSTER_GROUP_ID = ConfigurationManager.getProperty("kafka.cluster.group.id");

    /**
      *   =====> Redis configuration  <=====
      */
    String REDIS_LOCAL_MASTERNAME = ConfigurationManager.getProperty("redis.local.masterName");
    String REDIS_LOCAL_HOSTNAME = ConfigurationManager.getProperty("redis.local.hostname");
    String REDIS_LOCAL_PORT = ConfigurationManager.getProperty("redis.local.port");
    String REDIS_LOCAL_PASSWORD = ConfigurationManager.getProperty("redis.local.password");

    /**
      *   =====>  ES configuration <=====
      */
    String ES_CLUSTER_NAME_CONFIG = "cluster.name";
    String ES_BULK_FLUSH_MAX_ACTIONS_CONFIG = "bulk.flush.max.actions";

    String ES_CLUSTER_NAME = "es.cluster.name";
    String ES_BULK_FLUSH_MAX_ACTIONS = "es.bulk.flush.max.actions";
    String ES_INETADDRESS_NAME_1 = "es.inetAddress.name.1";
    String ES_INETADDRESS_NAME_2 = "es.inetAddress.name.2";

    /**
      *  =====>  HBase configuration  <=====
      */
    String HBASE_LOCAL_ZOOKEEPER_QUORUM = "hbase.local.zookeeper.quorum";
    String HBASE_CLUSTER_ZOOKEEPER_QUORUM = "hbase.cluster.zookeeper.quorum";
    String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = "hbase.zookeeper.property.clientport";

    /**
      * =====>Thread_pool configuration  <=====
      */
    String THEAD_NUM = "thread.num";

    /**
      * =====>  HTable configuration  <=====
      */
    String HTABLE_NAME = "htable.name";
    String HTABLE_COLUMNFAMILY = "htable.columnfamily";
    String HTABLE_COLUMN = "htable.column";

    /**
      *  =====>  My data_ts gap <=====
      */
    String TIME_20190508140000 = "time.20190508140000";
    String TIME_20190508141500 = "time.20190508141500";
    String TIME_20190508143000 = "time.20190508143000";
    String TIME_20190508144500 = "time.20190508144500";
    String TIME_20190508150000 = "time.20190508150000";
    String TIME_20190508151500 = "time.20190508151500";
    String TIME_20190508153000 = "time.20190508153000";
    String TIME_20190508154500 = "time.20190508154500";
    String TIME_20190508160000 = "time.20190508160000";
    String TIME_20190508161500 = "time.20190508161500";
    String TIME_20190508163000 = "time.20190508163000";
    String TIME_20190508164500 = "time.20190508164500";
    String TIME_20190508170000 = "time.20190508170000";



}
