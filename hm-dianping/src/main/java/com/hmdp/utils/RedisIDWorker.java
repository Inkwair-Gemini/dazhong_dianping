package com.hmdp.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
@Component
public class RedisIDWorker {
    private static final long BASE_TIMESTAMP = 1640995200;
    private static final int COUNT_BITS = 32;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    //生成唯一订单id ：时间戳 + 当前第几个订单
    public long nextId(String keyPrefix){
        //1.生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowTimeStamp = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowTimeStamp - BASE_TIMESTAMP;
        //2.生成序列号
        String date = now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //根据key生成序列号（keyPrefix为业务号，date为日期）;
        // number为key业务在date日期的订单数
        long number = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        //3.拼接返回
        return timestamp << COUNT_BITS | number;
    }

    public static void main(String[] args) {
        LocalDateTime beginTime = LocalDateTime.of(2022,1,1,0,0,0);
        long timestamp = beginTime.toEpochSecond(ZoneOffset.UTC);
        System.out.println(timestamp);
    }
}
