package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;

import com.hmdp.utils.RegexUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import javax.servlet.http.HttpSession;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone) {
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("无效的手机号！");
        }
        String code = RandomUtil.randomNumbers(6);
        //将电话和验证码存入redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //模拟发送验证码
        log.debug("验证码:" + code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm) {
        String phone = loginForm.getPhone();
        String code = loginForm.getCode();
        //1 验证手机号和验证码
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("无效的手机号！");
        }
        if(loginForm.getCode() == null || !stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY+phone).equals(code)){
            return Result.fail("验证码错误！");
        }

        User user = query().eq("phone",phone).one();
        //2 用户不存在就注册
        if(user == null){
            user = createUserWithPhone(phone);
        }
        //3 生成token
        String token = UUID.randomUUID().toString(true);
        String tokenKey = LOGIN_USER_KEY + token;

        UserDTO dto = BeanUtil.copyProperties(user, UserDTO.class);
        //将user信息转换为map方便redis存储
        Map<String, Object> usermap = BeanUtil.beanToMap(dto,new HashMap<>(),
                CopyOptions
                        .create()
                        .ignoreNullValue()
                        //id属性是Long，需要将Long转为string，因为是stringRedisTemplate
                        .setFieldValueEditor((fieldName,fieldValue)->fieldValue.toString()));
        //4 将token + 用户基本信息存入redis
        stringRedisTemplate.opsForHash().putAll(tokenKey, usermap);
        stringRedisTemplate.expire(tokenKey,LOGIN_USER_TTL,TimeUnit.MINUTES);

        return Result.ok(token);
    }
    private User createUserWithPhone(String phone){
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(8));
        this.save(user);
        return user;
    }
}
