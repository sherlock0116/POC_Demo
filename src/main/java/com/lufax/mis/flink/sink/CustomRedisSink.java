package com.lufax.mis.flink.sink;

import com.google.common.collect.Lists;
import com.lufax.mis.pool.RedisConnectionPool;
import com.lufax.mis.utils.RowUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Descriptor:
 * Author: sherlock
 * Date: 2019-08-02 3:48 PM
 */
public class CustomRedisSink extends RichSinkFunction<Row>{

    private Jedis jedis;
    private List<String> features;

    @Override
    public void setRuntimeContext(RuntimeContext t) {
        super.setRuntimeContext(t);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        JedisSentinelPool jedisSentinelPool = RedisConnectionPool.getInstance().init();
        jedis = jedisSentinelPool.getResource();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (jedis != null) {
            jedis.close();
        }
    }

    @Override
    public void invoke(Row row, Context context) {
        Iterator<Object> iterator = RowUtils.toIterable(row);
        ArrayList<Object> rowList = Lists.newArrayList(iterator);
        String userId = String.valueOf(rowList.get(0));
        for (int i = 0; i < row.getArity(); i++) {
            String val = String.valueOf(rowList.get(i));
            String feature = features.get(i);
            jedis.hset(userId, feature, val);
            if (jedis.ttl(userId) == -1) {
                jedis.expire(userId, 60*10);
            }
        }
    }
}
