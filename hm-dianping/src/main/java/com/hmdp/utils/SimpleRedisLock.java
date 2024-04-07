package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOCK_PREFIX;

public class SimpleRedisLock implements ILock{
    private StringRedisTemplate stringRedisTemplate;
    private String lockPrefix;
    private String lockName;
    private String uuid = UUID.randomUUID().toString(true);

    public SimpleRedisLock(String lockName,StringRedisTemplate stringRedisTemplate){
        this.lockPrefix = LOCK_PREFIX;
        this.stringRedisTemplate = stringRedisTemplate;
        this.lockName = lockName;
    }

    public SimpleRedisLock(String lockPrefix,String lockName, StringRedisTemplate stringRedisTemplate){
        this.lockPrefix = lockPrefix;
        this.lockName = lockName;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean tryLock(long timeSecond) {
        String threadId = uuid + "-" + Thread.currentThread().getId();

        Boolean aBoolean = stringRedisTemplate.opsForValue().setIfAbsent(lockPrefix + lockName, threadId, timeSecond, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(aBoolean);
    }

    /**
     * 加载Lua脚本
     */
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;

    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    @Override
    public void unlock() {
        stringRedisTemplate.execute(
                UNLOCK_SCRIPT,
                Collections.singletonList(lockPrefix + lockName),
                uuid + Thread.currentThread().getId()
        );
    }
}
