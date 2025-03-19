package cn.edu.sustech.service;

import cn.edu.sustech.entity.Answer;
import cn.edu.sustech.mapper.AnswerMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

@Service
public class AnswerService {

  @Autowired private AnswerMapper mapper;

  public AnswerMapper getMapper() {
    return mapper;
  }

  public List<Answer> acceptedAnswerByQuestionId(int questionID) {
    QueryWrapper<Answer> wrapper = new QueryWrapper<Answer>();
    wrapper.eq("question_id", questionID).eq("is_accepted", true).last("limit 1");
    return mapper.selectList(wrapper);
  }

  public List<Answer> answerByQuestionID(int questionID) {
    QueryWrapper<Answer> wrapper = new QueryWrapper<Answer>();
    wrapper.eq("question_id", questionID);
    return mapper.selectList(wrapper);
  }

  public List<Answer> answerByDate(Date from, Date end) {
    QueryWrapper<Answer> wrapper = new QueryWrapper<Answer>();
    wrapper.between("creation_date", from, end);
    return mapper.selectList(wrapper);
  }

  public List<Answer> answerByAnswerId(int answerID) {
    QueryWrapper<Answer> wrapper = new QueryWrapper<Answer>();
    wrapper.eq("answer_id", answerID);
    return mapper.selectList(wrapper);
  }
  
  public List<Answer> acceptedAnswer(Date from, Date end) {
    QueryWrapper<Answer> wrapper = new QueryWrapper<Answer>();
    wrapper.between("creation_date", from, end).eq("is_accepted", true);
    return mapper.selectList(wrapper);
  }
  
}
