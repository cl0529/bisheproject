package com.project.demo.dao;

import com.project.demo.dao.base.BaseMapper;
import com.project.demo.entity.InheritingUsers;
import org.apache.ibatis.annotations.Mapper;

/**
 * 传承用户：(InheritingUsers)Mapper接口
 *
 */
@Mapper
public interface InheritingUsersMapper extends BaseMapper<InheritingUsers>{

}
