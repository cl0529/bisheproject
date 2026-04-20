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
 * 非遗项目：(IntangibleCulturalHeritageProjects)表实体类
 *
 */
@TableName("`intangible_cultural_heritage_projects`")
@Data
@EqualsAndHashCode(callSuper = false)
public class IntangibleCulturalHeritageProjects implements Serializable {

    // IntangibleCulturalHeritageProjects编号
    @TableId(value = "intangible_cultural_heritage_projects_id", type = IdType.AUTO)
    private Integer intangible_cultural_heritage_projects_id;

    // 项目名称
    @TableField(value = "`entry_name`")
    private String entry_name;
    // 地区名称
    @TableField(value = "`regional_name`")
    private String regional_name;
    // 项目类别
    @TableField(value = "`project_category`")
    private String project_category;
    // 封面图片
    @TableField(value = "`cover_photo`")
    private String cover_photo;
    // 传承人员
    @TableField(value = "`inheritance_personnel`")
    private Integer inheritance_personnel;
    // 人员姓名
    @TableField(value = "`personnel_name`")
    private String personnel_name;
    // 公布日期
    @TableField(value = "`announcement_date`")
    private Timestamp announcement_date;
    // 申报单位
    @TableField(value = "`declaration_unit`")
    private String declaration_unit;
    // 保护单位
    @TableField(value = "`protecting_units`")
    private String protecting_units;
    // 详细介绍
    @TableField(value = "`detailed_introduction`")
    private String detailed_introduction;

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
