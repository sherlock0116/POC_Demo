package com.lufax.mis.pool;

import com.google.common.collect.Sets;
import com.lufax.mis.constant.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisSentinelPool;

import java.util.HashSet;

/**
 * Descriptor:
 * Author: sherlock
 * Date: 2019-08-02 3:58 PM
 */
public class RedisConnectionPool {

    private static final Logger LOGGER = LoggerFactory.getLogger(RedisConnectionPool.class);
    private static RedisConnectionPool instance;
    private JedisSentinelPool jedisSentinelPool;

    private RedisConnectionPool() { }

    public static RedisConnectionPool getInstance() {
        if (instance == null) {
            instance = new RedisConnectionPool();
        }
        return instance;
    }

    public JedisSentinelPool init() {
        if (jedisSentinelPool == null) {
            String masterName = Constants.REDIS_LOCAL_MASTERNAME;
            String hostName = Constants.REDIS_LOCAL_HOSTNAME;
            int port = Integer.valueOf(Constants.REDIS_LOCAL_PORT);
            String password = Constants.REDIS_LOCAL_PASSWORD;
            HashSet<String> hostAndPort = Sets.newHashSet();
            hostAndPort.add(new HostAndPort(hostName, port).toString());
            jedisSentinelPool = new JedisSentinelPool(masterName, hostAndPort, password);
        }
        return jedisSentinelPool;
    }
}
