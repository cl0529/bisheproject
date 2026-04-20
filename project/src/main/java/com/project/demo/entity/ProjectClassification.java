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
 * 项目分类：(ProjectClassification)表实体类
 *
 */
@TableName("`project_classification`")
@Data
@EqualsAndHashCode(callSuper = false)
public class ProjectClassification implements Serializable {

    // ProjectClassification编号
    @TableId(value = "project_classification_id", type = IdType.AUTO)
    private Integer project_classification_id;

    // 项目类别
    @TableField(value = "`project_category`")
    private String project_category;










    // 更新时间
    @TableField(value = "update_time")
    private Timestamp update_time;

    // 创建时间
    @TableField(value = "create_time")
    private Timestamp create_time;











}
