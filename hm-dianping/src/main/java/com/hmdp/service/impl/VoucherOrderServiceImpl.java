package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIDWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.annotations.ResultType;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.hmdp.utils.RedisConstants.LOCK_VOUCHERORDER_KEY;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Autowired
    private ISeckillVoucherService iSeckillVoucherService;

    @Autowired
    private RedisIDWorker redisIDWorker;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Autowired
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
    /**
     * VoucherOrderServiceImpl类的代理对象
     * 将代理对象的作用域进行提升，方面子线程取用
     */
    private IVoucherOrderService proxy;

    // 线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    // 类初始化就启动异步线程
    @PostConstruct
    private void initialize() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    // 阻塞队列
//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue(1024 * 1024);

    // 线程
    private class VoucherOrderHandler implements Runnable {
        String queueName= "stream.orders";
        String groupName= "g1";
        @Override
        public void run() {
            while (true) {
                try{
                    //1.尝试从消息队列中获取消息
                    // XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAM stream.order >
                    List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(
                            Consumer.from(groupName, "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2.如果没有，则循环监听
                    if (records == null || records.isEmpty()){
                        continue;
                    }
                    //3.如果有，就处理消息
                    MapRecord<String, Object, Object> record = records.get(0);
                    RecordId recordId = record.getId();
                    Map<Object,Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values,new VoucherOrder(),true);

                    handleVoucherOrder(voucherOrder);

                    //4.反馈确认
                    // SACK stream.order g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,groupName,recordId);
                }catch (Exception e){
                    // 取出消息时异常，还没反馈成功
                    handlePendingList();
                }
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder){
        //使用redisson轮子 获取锁
        Long userId = voucherOrder.getUserId();
        RLock lock = redissonClient.getLock(LOCK_VOUCHERORDER_KEY + userId);
        boolean isLock = lock.tryLock();

        if (!isLock) {
            //有人捷足先登了，但可能还没生成订单
            log.error("该账号正在或已完成购买！");
        }
        try {
            proxy.createOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    private void handlePendingList() {
        String queueName = "stream.orders";
        String groupName = "g1";
        while (true) {
            try {
                //1.尝试从pending-list中获取消息,消息为已拿取但未确认的
                // XREADGROUP GROUP g1 c1 COUNT 1 STREAM stream.order 0
                List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream().read(
                        Consumer.from(groupName, "c1"),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create(queueName, ReadOffset.from("0"))
                );
                //2.如果没有，则说明没有未响应的消息
                if (records == null || records.isEmpty()) {
                    break;
                }
                //3.如果有，就处理消息
                MapRecord<String, Object, Object> record = records.get(0);
                RecordId recordId = record.getId();
                Map<Object, Object> values = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);

                handleVoucherOrder(voucherOrder);

                //4.反馈确认
                // SACK stream.order g1 id
                stringRedisTemplate.opsForStream().acknowledge(queueName, groupName, recordId);
            } catch (Exception e) {
                // 取出消息时异常，还没反馈成功
                log.error("处理订单异常", e);
                handlePendingList();
            }
        }
    }

    // 用户购买优惠券
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = redisIDWorker.nextId("order");
        // 1,执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
        voucherId.toString(), userId.toString(),String.valueOf(orderId));

        int r = result.intValue();
        // 2.判断结果
        if (r != 0) {
            // result为1表示库存不足，result为2表示用户已下单
            return Result.fail(r == 2 ? "不能重复下单" : "库存不足");
        }

        /*Spring的@Transactional注解要想事务生效，必须使用动态代理。Service中一个方法中调用另一个方法，
        另一个方法使用了事务，此时会导致@Transactional失效，所以我们需要创建一个代理对象，使用代理对象来调用方法。*/
        //给当前类的代理对象赋值
        this.proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 4.返回订单id
        return Result.ok(orderId);
    }

    @Override
    @Transactional
    public void createOrder(VoucherOrder order){
        Long userId = order.getId();
        Long voucherId = order.getVoucherId();
        int count = this.query().eq("user_id",userId).eq("voucher_id",voucherId).count();
        if(count > 0){
            //有人捷足先登且完成订单了
            log.error("用户已经购买过一次！");
        }
        //6. 扣除库存
        boolean success = iSeckillVoucherService.update()
                .setSql("stock = stock -1")
                .eq("voucher_id", voucherId)
                // CAS 乐观锁
                .gt("stock",0)
                .update();
        if(!success){
            log.error("优惠券库存不足");
        }
        //7. 生成订单
        this.save(order);
    }
}
