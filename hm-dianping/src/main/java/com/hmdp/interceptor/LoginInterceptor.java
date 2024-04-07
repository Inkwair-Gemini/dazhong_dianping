package com.hmdp.interceptor;


import cn.hutool.http.HttpStatus;
import com.hmdp.utils.UserHolder;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
//对后续需要登陆才能访问的请求进行验证
public class LoginInterceptor implements HandlerInterceptor {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //如果线程变量里为null，则拦截
        if(UserHolder.getUser() == null) {
            response.setStatus(HttpStatus.HTTP_UNAUTHORIZED);
            return false;
        }
        return true;
    }
}
