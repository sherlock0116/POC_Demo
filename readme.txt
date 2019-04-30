1. Kafka数据生产者: test/com.lufax.mis.KafkaMsgProducer

2. Kafka中实时流数据为Json格式:
    {un:206, screen:myAccount, title:null,   point_type:screen, click:0, t_ms:1556019567957}
    {un:184, screen:find,      title:bannel, point_type:event,  click:1, t_ms:1556019567958}

3. 参数配置 my.properties

4. 当前进度:
    4.1 完成Flink_SQL对曝光页/点击位的pv、uv统计

5. 下周内容: 实现HBase配置SQL,灵活调度Flink_SQL程序进行pv/uv查询