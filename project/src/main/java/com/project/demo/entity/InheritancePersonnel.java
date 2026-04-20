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
 * 传承人员：(InheritancePersonnel)表实体类
 *
 */
@TableName("`inheritance_personnel`")
@Data
@EqualsAndHashCode(callSuper = false)
public class InheritancePersonnel implements Serializable {

    // InheritancePersonnel编号
    @TableId(value = "inheritance_personnel_id", type = IdType.AUTO)
    private Integer inheritance_personnel_id;

    // 项目名称
    @TableField(value = "`entry_name`")
    private String entry_name;
    // 项目类别
    @TableField(value = "`project_category`")
    private String project_category;
    // 传承用户
    @TableField(value = "`inheriting_users`")
    private Integer inheriting_users;
    // 人员姓名
    @TableField(value = "`personnel_name`")
    private String personnel_name;
    // 人员年龄
    @TableField(value = "`age_of_personnel`")
    private String age_of_personnel;
    // 人员性别
    @TableField(value = "`gender_of_personnel`")
    private String gender_of_personnel;
    // 封面图片
    @TableField(value = "`cover_photo`")
    private String cover_photo;
    // 出生日期
    @TableField(value = "`date_of_birth`")
    private Timestamp date_of_birth;
    // 申报地区
    @TableField(value = "`declaration_region`")
    private String declaration_region;
    // 个人简介
    @TableField(value = "`personal_profile`")
    private String personal_profile;

    // 点击数
    @TableField(value = "hits")
    private Integer hits;

    // 点赞数
    @TableField(value = "praise_len")
    private Integer praise_len;








    // 更新时间
    @TableField(value = "update_time")
    private Timestamp update_time;

    // 创建时间
    @TableField(value = "create_time")
    private Timestamp create_time;











}
