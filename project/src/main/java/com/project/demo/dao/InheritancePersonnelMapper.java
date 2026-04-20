package com.project.demo.dao;

import com.project.demo.dao.base.BaseMapper;
import com.project.demo.entity.InheritancePersonnel;
import org.apache.ibatis.annotations.Mapper;

/**
 * 传承人员：(InheritancePersonnel)Mapper接口
 *
 */
@Mapper
public interface InheritancePersonnelMapper extends BaseMapper<InheritancePersonnel>{

}
