package com.hmdp.utils;

import static com.hmdp.utils.RedisConstants.LOCK_PREFIX;

public interface ILock {
    boolean tryLock(long timeSecond);
    void unlock();
}
