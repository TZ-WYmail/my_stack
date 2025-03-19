package cn.edu.sustech.service;

import cn.edu.sustech.entity.Question;
import cn.edu.sustech.mapper.QuestionMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

@Service
public class QuestionService {

  @Autowired private QuestionMapper mapper;

  public QuestionMapper getMapper() {
    return mapper;
  }

  public List<Question> totalQuestion(Date from, Date end) {
    QueryWrapper<Question> wrapper = new QueryWrapper<Question>();
    wrapper.ge("creation_date", from).le("creation_date", end);
    return mapper.selectList(wrapper);
  }

  public List<Question> noAnswer(Date from, Date end) {
    QueryWrapper<Question> wrapper = new QueryWrapper<Question>();
    wrapper.ge("creation_date", from).le("creation_date", end).eq("answer_count", 0);
    return mapper.selectList(wrapper);
  }

  public List<Question> withAnswer(Date from, Date end) {
    QueryWrapper<Question> wrapper = new QueryWrapper<Question>();
    wrapper.ge("creation_date", from).le("creation_date", end).gt("answer_count", 0);
    return mapper.selectList(wrapper);
  }

  public Question questionById(int questionID) {
    return mapper.selectById(questionID);
  }
}
