package cn.edu.sustech.controller;

import cn.edu.sustech.entity.Answer;
import cn.edu.sustech.entity.Comment;
import cn.edu.sustech.entity.Question;
import cn.edu.sustech.service.AnswerService;
import cn.edu.sustech.service.CommentService;
import cn.edu.sustech.service.QuestionService;
import cn.edu.sustech.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
@RequestMapping("/api/user")
public class UserController {

  @Autowired private UserService userService;
  @Autowired private AnswerService answerService;
  @Autowired private CommentService commentService;
  @Autowired private QuestionService questionService;

  @GetMapping("/post-answer-distribution")
  public List<int[]> getPostAnswerDistribution(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Answer> answers = answerService.answerByDate(from, end);
    Map<Integer, HashSet<Integer>> map = new HashMap<>();
    answers.forEach(
        answer -> {
          map.merge(
              answer.getQuestionId(),
              new HashSet<>(Collections.singletonList(answer.getAccountId())),
              (oldValue, newValue) -> {
                oldValue.addAll(newValue);
                return oldValue;
              });
        });
    Map<Integer, Integer> result = new HashMap<>();
    map.forEach((key, value) -> result.merge(value.size(), 1, Integer::sum));
    List<int[]> resultList = new ArrayList<>();
    result.forEach((key, value) -> resultList.add(new int[] {key, value}));
    return resultList;
  }

  @GetMapping("/post-comment-distribution")
  public List<int[]> getPostCommentDistribution(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Comment> comments = commentService.commentByDate(from, end);
    Map<Integer, HashSet<Integer>> map = new HashMap<>();
    comments.forEach(
        comment -> {
          List<Answer> answers = answerService.answerByAnswerId(comment.getPostId());
          if (answers != null && !answers.isEmpty()) {
            answers.forEach(
                answer -> {
                  map.merge(
                      answer.getQuestionId(),
                      new HashSet<>(Collections.singletonList(answer.getAccountId())),
                      (oldValue, newValue) -> {
                        oldValue.addAll(newValue);
                        return oldValue;
                      });
                });
          } else {
            map.merge(
                comment.getPostId(),
                new HashSet<>(Collections.singletonList(comment.getAccountId())),
                (oldValue, newValue) -> {
                  oldValue.addAll(newValue);
                  return oldValue;
                });
          }
        });
    Map<Integer, Integer> result = new HashMap<>();
    map.forEach((key, value) -> result.merge(value.size(), 1, Integer::sum));
    List<int[]> resultList = new ArrayList<>();
    result.forEach((key, value) -> resultList.add(new int[] {key, value}));
    return resultList;
  }

  @GetMapping("/participation-distribution")
  public List<int[]> getParticipationDistribution(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Answer> answers = answerService.answerByDate(from, end);
    Map<Integer, HashSet<Integer>> map = new HashMap<>();
    answers.forEach(
        answer -> {
          map.merge(
              answer.getQuestionId(),
              new HashSet<>(Collections.singletonList(answer.getAccountId())),
              (oldValue, newValue) -> {
                oldValue.addAll(newValue);
                return oldValue;
              });
        });
    List<Comment> comments = commentService.commentByDate(from, end);
    comments.forEach(
        comment -> {
          List<Answer> answers2 = answerService.answerByAnswerId(comment.getPostId());
          if (answers2 != null && !answers2.isEmpty()) {
            answers2.forEach(
                answer -> {
                  map.merge(
                      answer.getQuestionId(),
                      new HashSet<>(Collections.singletonList(answer.getAccountId())),
                      (oldValue, newValue) -> {
                        oldValue.addAll(newValue);
                        return oldValue;
                      });
                });
          } else {
            map.merge(
                comment.getPostId(),
                new HashSet<>(Collections.singletonList(comment.getAccountId())),
                (oldValue, newValue) -> {
                  oldValue.addAll(newValue);
                  return oldValue;
                });
          }
        });
    Map<Integer, Integer> result = new HashMap<>();
    map.forEach((key, value) -> result.merge(value.size(), 1, Integer::sum));
    List<int[]> resultList = new ArrayList<>();
    result.forEach((key, value) -> resultList.add(new int[] {key, value}));
    return resultList;
  }

  @GetMapping("/activity")
  public List<Map<String, Object>> getUserActivity(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    Map<Integer, Integer> map = new HashMap<>();
    List<Question> questions = questionService.totalQuestion(from, end);
    questions.forEach(
        question -> {
          map.merge(question.getAccountId(), 1, Integer::sum);
        });
    List<Answer> answers = answerService.answerByDate(from, end);
    answers.forEach(
        answer -> {
          map.merge(answer.getAccountId(), 1, Integer::sum);
        });
    List<Comment> comments = commentService.commentByDate(from, end);
    comments.forEach(
        comment -> {
          map.merge(comment.getAccountId(), 1, Integer::sum);
        });
    List<Map<String, Object>> resultList = new ArrayList<>();
    map.entrySet().stream()
        .sorted(Map.Entry.<Integer, Integer>comparingByValue().reversed())
        .forEach(
            e -> {
              Map<String, Object> result = new HashMap<>();
              result.put("user", userService.userById(e.getKey()));
              result.put("activity", e.getValue());
              resultList.add(result);
            });
    return resultList;
  }
}
