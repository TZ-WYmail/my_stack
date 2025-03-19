package cn.edu.sustech.service;

import cn.edu.sustech.entity.*;
import cn.edu.sustech.mapper.AnswerMapper;
import cn.edu.sustech.mapper.CommentMapper;
import cn.edu.sustech.mapper.QuestionMapper;
import cn.edu.sustech.mapper.api.AnswerApiMapper;
import cn.edu.sustech.mapper.api.CommentApiMapper;
import cn.edu.sustech.mapper.api.QuestionApiMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Service
public class ApiService {

  @Autowired private QuestionApiMapper questionApiMapper;

  @Autowired private CommentApiMapper commentApiMapper;

  @Autowired private AnswerApiMapper answerApiMapper;

  @Autowired private QuestionMapper questionMapper;

  @Autowired private AnswerMapper answerMapper;

  @Autowired private CommentMapper commentMapper;

  public Map<String, Integer> getApiCount(Date from, Date end) {
    HashMap<String, Integer> map = new HashMap<>();
    QueryWrapper<Question> questionQueryWrapper = new QueryWrapper<>();
    questionQueryWrapper.ge("creation_date", from).le("creation_date", end).select("question_id");
    questionMapper
        .selectList(questionQueryWrapper)
        .forEach(
            question -> {
              QueryWrapper<QuestionApiCount> questionApiCountQueryWrapper = new QueryWrapper<>();
              questionApiCountQueryWrapper.eq("question_id", question.getQuestionId());
              questionApiMapper
                  .selectList(questionApiCountQueryWrapper)
                  .forEach(
                      questionApiCount -> {
                        map.merge(
                            questionApiCount.getApiName(),
                            questionApiCount.getCount(),
                            Integer::sum);
                      });
            });
    QueryWrapper<Answer> answerQueryWrapper = new QueryWrapper<>();
    answerQueryWrapper.ge("creation_date", from).le("creation_date", end).select("question_id");
    answerMapper
        .selectList(answerQueryWrapper)
        .forEach(
            answer -> {
              QueryWrapper<AnswerApiCount> answerApiCountQueryWrapper = new QueryWrapper<>();
              answerApiCountQueryWrapper.eq("answer_id", answer.getAnswerId());
              answerApiMapper
                  .selectList(answerApiCountQueryWrapper)
                  .forEach(
                      answerApiCount -> {
                        map.merge(
                            answerApiCount.getApiName(), answerApiCount.getCount(), Integer::sum);
                      });
            });
    QueryWrapper<Comment> commentQueryWrapper = new QueryWrapper<>();
    commentQueryWrapper.ge("creation_date", from).le("creation_date", end).select("comment_id");
    commentMapper
        .selectList(commentQueryWrapper)
        .forEach(
            comment -> {
              QueryWrapper<CommentApiCount> commentApiCountQueryWrapper = new QueryWrapper<>();
              commentApiCountQueryWrapper.eq("comment_id", comment.getCommentId());
              commentApiMapper
                  .selectList(commentApiCountQueryWrapper)
                  .forEach(
                      commentApiCount -> {
                        map.merge(
                            commentApiCount.getApiName(), commentApiCount.getCount(), Integer::sum);
                      });
            });
    return map;
  }
}
