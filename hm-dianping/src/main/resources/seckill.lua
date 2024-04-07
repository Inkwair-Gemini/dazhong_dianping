--- Description 判断库存是否充足 && 判断用户是否已下单
--- 1.不充足  2.已下单  0.可以下单

-- 优惠券id
local voucherId = ARGV[1];
-- 用户id
local userId = ARGV[2];
-- 订单id
local orderId = ARGV[3];

-- 库存的key
local stockKey = 'seckill:stock:' .. voucherId;
-- 订单key
local orderKey = 'seckill:order:' .. voucherId;

-- 判断库存是否充足 get stockKey > 0 ?
local stock = redis.call("GET",stockKey);
-- 不存在这个key的话返回false
if(stock == false or tonumber(stock) <= 0) then
    return 1;
end
-- 库存充足，判断用户是否已经下过单 SISMEMBER orderKey userId
if(redis.call("SISMEMBER",orderKey,userId) == 1)then
    return 2;
end
-- 库存充足，没有下过单，扣库存、下单
redis.call("INCRBY",stockKey,-1);
redis.call("sadd",orderKey,userId);

-- 往消息队列中放入消息
redis.call("xadd","stream.orders","*","userId",userId,"voucherId",voucherId,"id",orderId);

return 0;