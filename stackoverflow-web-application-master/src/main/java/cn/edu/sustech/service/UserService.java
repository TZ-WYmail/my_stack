package cn.edu.sustech.service;

import cn.edu.sustech.entity.User;
import cn.edu.sustech.mapper.UserMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class UserService {

  @Autowired private UserMapper userMapper;

  public User userById(int id) {
    return userMapper.selectById(id);
  }
}
