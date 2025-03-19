package cn.edu.sustech.service;

import cn.edu.sustech.entity.Comment;
import cn.edu.sustech.mapper.CommentMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

@Service
public class CommentService {

  @Autowired private CommentMapper mapper;

  public List<Comment> commentByDate(Date from, Date end) {
    QueryWrapper<Comment> wrapper = new QueryWrapper<Comment>();
    wrapper.between("creation_date", from, end);
    return mapper.selectList(wrapper);
  }
}
