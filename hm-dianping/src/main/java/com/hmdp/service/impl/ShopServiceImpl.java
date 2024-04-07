package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SimpleRedisLock;
import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.time.LocalDateTime;
import java.util.concurrent.*;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedissonClient redissonClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
//        Shop shop = queryWithPassThrough(id);
        //互斥锁解决缓存击穿（缓存穿透也有）
//        Shop shop = queryWithMutex(id);
        //逻辑过期策略解决缓存击穿
        Shop shop = queryWithLogicalExpire(id);
        if (shop == null) {
            return Result.fail("店铺不存在");
        }
        return Result.ok(shop);
    }

    @Override
    @Transactional
    public Result update(Shop newShop) {
        Long id = newShop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空！");
        }
        //1 先更新（通过id）
        updateById(newShop);
        String idKey = CACHE_SHOP_KEY + newShop.getId();
        //2 再删除
        stringRedisTemplate.delete(idKey);
        return Result.ok();
    }

    public Shop queryWithPassThrough(Long id) {
        String key = CACHE_SHOP_KEY + id;
        //1 先从redis拿，如果有(并且不为“”)就返回结果
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //2 如果redis里有,但是是“”,返回错误
        if (shopJson != null) {
            return null;
        }
        //3 没有就去数据库拿
        Shop shop = this.getById(id);
        if (shop == null) {
            //3.1 数据库也没有的话，就返回错误，并在redis中设“”，防止缓存穿透
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //3.2 数据库里有的话就返回并存入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    public Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;

        //1 先从redis拿，如果有(并且不为“”)就返回结果
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //2 如果redis里有,但是是“”,返回错误
        if (shopJson != null) {
            return null;
        }

        //3 缓存重建
        //3.1 获取锁
        Shop shop = null;
//        RLock lock = redissonClient.getLock(LOCK_SHOP_KEY+id);
        SimpleRedisLock lock= new SimpleRedisLock(LOCK_SHOP_KEY,id+"",stringRedisTemplate);
        try {
            boolean isLock = lock.tryLock(LOCK_SHOP_TTL);
            //3.2 判断是否成功
            if (!isLock) {
                //3.3 失败，休眠后重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            Thread.sleep(200);
            //3.4 成功，查数据库
            shop = this.getById(id);

            //5 //数据库也没有的话，就返回错误，并在redis中设“”，防止缓存穿透
            if (shop == null) {
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //6 数据库里有的话就返回并存入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        } finally {
            //7 释放锁
            lock.unlock();
        }
        return shop;
    }
    private static final ThreadPoolExecutor THREAD_POOL = new ThreadPoolExecutor(
            5, 10, 2L, TimeUnit.MINUTES, new ArrayBlockingQueue<>(300)
    );
    //另一种创建线程池的方法
    private static final ExecutorService THREAD_POOL_EXECUTOR = Executors.newFixedThreadPool(10);

    public Shop queryWithLogicalExpire(Long id){
        String key = CACHE_SHOP_KEY + id;
        // 1、从Redis中查询店铺数据，并判断缓存是否命中
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isBlank(shopJson)) {
            // 1.1 缓存未命中，直接返回失败信息
            return null;
        }
        // 1.2 缓存命中，将JSON字符串反序列化未对象，并判断缓存数据是否逻辑过期
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        // 这里需要先转成JSONObject再转成反序列化，否则可能无法正确映射Shop的字段
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 当前缓存数据未过期，直接返回
            return shop;
        }

        // 2、缓存数据已过期，获取互斥锁，并且重建缓存
        SimpleRedisLock lock = new SimpleRedisLock(LOCK_SHOP_KEY,id+"",stringRedisTemplate);
        boolean isLock = lock.tryLock(LOCK_SHOP_TTL);
        if (isLock) {
            // 获取锁成功，开启一个子线程去重建缓存
            THREAD_POOL.execute(new Runnable() {
                @Override
                public void run() {
                    //从数据库更新redis
                    saveShopToCache(id,30L);
                    lock.unlock();
                }
            });
        }
        // 4、无论是否有锁都要返回过期数据
        return shop;
    }

    //数据预热 从数据库查找数据存入redis
    public void saveShopToCache(Long id, Long expireSeconds) {
        Shop shop = this.getById(id);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        String jsonStr = JSONUtil.toJsonStr(redisData);
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, jsonStr);
    }
}
