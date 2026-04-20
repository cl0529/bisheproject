package com.project.demo.entity;

import com.alibaba.fastjson.annotation.JSONField;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.*;

import java.io.Serializable;
import java.sql.Timestamp;


/**
 * 传承用户：(InheritingUsers)表实体类
 *
 */
@TableName("`inheriting_users`")
@Data
@EqualsAndHashCode(callSuper = false)
public class InheritingUsers implements Serializable {

    // InheritingUsers编号
    @TableId(value = "inheriting_users_id", type = IdType.AUTO)
    private Integer inheriting_users_id;

    // 人员姓名
    @TableField(value = "`personnel_name`")
    private String personnel_name;
    // 人员年龄
    @TableField(value = "`age_of_personnel`")
    private String age_of_personnel;
    // 人员性别
    @TableField(value = "`gender_of_personnel`")
    private String gender_of_personnel;
    // 家庭住址
    @TableField(value = "`home_address`")
    private String home_address;
    // 证件图片
    @TableField(value = "`id_picture`")
    private String id_picture;



    // 审核状态
    @TableField(value = "examine_state")
    private String examine_state;




    // 用户编号
    @TableField(value = "user_id")
    private Integer userId;



    // 更新时间
    @TableField(value = "update_time")
    private Timestamp update_time;

    // 创建时间
    @TableField(value = "create_time")
    private Timestamp create_time;











}
