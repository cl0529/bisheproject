package com.project.demo.controller;

import com.alibaba.fastjson.JSONObject;
import com.project.demo.controller.base.BaseController;
import com.project.demo.entity.CodeToken;
import com.project.demo.service.CodeTokenService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.mail.*;
import javax.mail.internet.MimeMessage;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 邮箱验证码：(Ad)表控制层
 */
@RestController
@RequestMapping("code_token")
public class CodeTokenController extends BaseController<CodeToken, CodeTokenService> {
    /**
     * 服务对象
     */
    @Autowired
    public CodeTokenController(CodeTokenService service) {
        setService(service);
    }

    /**
     * 发送验证码到邮箱API
     *
     * @param email
     * @return
     */
    @GetMapping("send_email")
    public Map<String, Object> sendEmail(String email) {
        //判断邮箱号是否为空
        if (StringUtils.isEmpty(email)) {
            return error(30000, "参数错误");
        }
        try {
            //随机生成 6位数的验证码
            int code = (int) (Math.floor(Math.random() * 900000) + 100000);
            // 设置SMTP服务器属性
            Properties properties = new Properties();
            properties.put("mail.smtp.host", "smtp.qq.com");
            properties.put("mail.smtp.port", 465);
            properties.put("mail.smtp.auth", "true");
            properties.put("mail.smtp.starttls.enable", "true");
            properties.put("mail.smtp.ssl.enable", true);

            // 新建一个认证器
            Authenticator auth = new Authenticator() {
                @Override
                public PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication("3216252476@qq.com", "puuqglgygxwmdghb");
                }
            };

            // 新建一个会话
            Session session = Session.getInstance(properties, auth);

            // 新建一个消息，并设置内容
            MimeMessage message = new MimeMessage(session);
            message.setContent("<h1>验证码为："+code+"</h1>", "text/html;charset=UTF-8");
            message.setHeader("Resent-From", "3216252476@qq.com");
            message.setHeader("From", "3216252476@qq.com");
            message.setRecipients(Message.RecipientType.TO, email);
            message.setSubject("找回密码");

            // 发送消息
            Transport.send(message);
            Calendar cal = Calendar.getInstance();
            cal.add(Calendar.MINUTE,10);
            String token = service.encryption(code + "_" + cal.getTime());
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Map<String, Object> map = new HashMap<>();
            map.put("token",token);
            map.put("code",code);
            map.put("expire_time",format.format(cal.getTime()));
            this.addMap(map);
            Map<String, Object> result = new HashMap<>();
            result.put("result",1);
            result.put("token",token);
            return result;
        }catch (MessagingException e){
            e.printStackTrace();
            return error(30000, e.getMessage());
        }
    }

    @GetMapping("verify_email_code")
    public Map<String, Object> verifyEmailCode(String token,String code) {
        if (StringUtils.isEmpty(token) || StringUtils.isEmpty(code)) {
            return error(30000, "参数错误");
        }else {
            Map<String,String> queryMap = new HashMap<>();
            queryMap.put("token",token);
            Map<String,String> configMap = new HashMap<>();
            configMap.put("like","false");
            List resultList = service.selectBaseList(service.select(queryMap, configMap));
            if (resultList.size() > 0) {
                CodeToken obj = (CodeToken) resultList.get(0);
                Date now = new Date();
                if (obj.getExpire_time().after(now)){
                    if (code.equals(obj.getCode())){
                        return success(1);
                    }else {
                        return error(30000, "验证码错误");
                    }
                }else {
                    return error(30000, "验证码已失效");
                }
            }else {
                return error(30000, "token请求错误");
            }
        }
    }

}


