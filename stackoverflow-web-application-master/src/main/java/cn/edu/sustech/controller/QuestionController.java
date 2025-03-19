package cn.edu.sustech.controller;

import cn.edu.sustech.entity.Answer;
import cn.edu.sustech.entity.Question;
import cn.edu.sustech.service.AnswerService;
import cn.edu.sustech.service.QuestionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

@RestController
@RequestMapping("/api/question")
public class QuestionController {

  @Autowired private QuestionService questionService;
  @Autowired private AnswerService answerService;

  @GetMapping("/no-answer/total")
  public int noAnswerQuery(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    return questionService.noAnswer(from, end).size();
  }

  @GetMapping("/with-answer/total")
  public int totalAnswerQuery(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    return questionService.withAnswer(from, end).size();
  }

  @GetMapping("/no-answer/percentage")
  public double noAnswerPercentageQuery(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Question> noAnswer = questionService.noAnswer(from, end);
    List<Question> total = questionService.totalQuestion(from, end);
    return (double) noAnswer.size() / total.size();
  }

  @GetMapping("/with-answer/percentage")
  public double totalAnswerPercentageQuery(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Question> noAnswer = questionService.withAnswer(from, end);
    List<Question> total = questionService.totalQuestion(from, end);
    return (double) (total.size() - noAnswer.size()) / total.size();
  }

  @GetMapping("/answer/average")
  public double answerAverageQuery(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Question> total = questionService.totalQuestion(from, end);
    return (double) total.stream().map(Question::getAnswerCount).reduce(0, Integer::sum)
        / total.size();
  }

  @GetMapping("/answer/max")
  public int answerMaxQuery(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Question> total = questionService.totalQuestion(from, end);
    return total.stream().map(Question::getAnswerCount).reduce(0, Integer::max);
  }

  @GetMapping("/answer/distribution")
  public List<int[]> answerDistributionQuery(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Question> total = questionService.totalQuestion(from, end);
    Map<Integer, Integer> distribution = new HashMap<Integer, Integer>();
    total.stream()
        .forEach(
            question -> {
              distribution.merge(question.getAnswerCount(), 1, Integer::sum);
            });
    List<int[]> result = new ArrayList<>();
    distribution.entrySet().stream()
        .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
        .forEach(
            entry -> {
              result.add(new int[] {entry.getKey(), entry.getValue()});
            });
    return result;
  }

  @GetMapping("/with-accepted-answer/total")
  public int withAcceptedAnswerQuery(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Question> withAcceptedAnswer = questionService.totalQuestion(from, end);
    Iterator<Question> iterator = withAcceptedAnswer.iterator();
    while (iterator.hasNext()) {
      Question question = iterator.next();
      List<Answer> answers = answerService.acceptedAnswerByQuestionId(question.getQuestionId());
      if (answers.size() == 0) {
        iterator.remove();
      }
    }
    return withAcceptedAnswer.size();
  }

  @GetMapping("/with-accepted-answer/percentage")
  public double withAcceptedAnswerPercentageQuery(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Question> withAnswerList = questionService.withAnswer(from, end);
    Iterator<Question> iterator = withAnswerList.iterator();
    while (iterator.hasNext()) {
      Question question = iterator.next();
      List<Answer> answers = answerService.acceptedAnswerByQuestionId(question.getQuestionId());
      if (answers.size() == 0) {
        iterator.remove();
      }
    }
    return (double) withAnswerList.size() / questionService.totalQuestion(from, end).size();
  }

  @GetMapping("/with-accepted-answer/resolution-distribution")
  public List<long[]> resolutionDistributionQuery(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Answer> acceptedAnswers = answerService.acceptedAnswer(from, end);
    Map<Long, Integer> distribution = new HashMap<>();
    acceptedAnswers.stream()
        .forEach(
            answer -> {
                Question question = questionService.questionById(answer.getQuestionId());
                distribution.merge(
                    answer.getCreationDate().getTime()
                        - question.getCreationDate().getTime(),
                    1,
                    Integer::sum);
            });
    List<long[]> result = new ArrayList<>();
    distribution.entrySet().stream()
        .sorted((e1, e2) -> e1.getKey().compareTo(e2.getKey()))
        .forEach(
            entry -> {
              result.add(new long[] {entry.getKey(), entry.getValue()});
            });
    return result;
  }

  @GetMapping("/with-accepted-answer/better-answer/total")
  public int withAcceptedAnswerBetterAnswerQuery(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Question> withAnswer = questionService.withAnswer(from, end);
    return withAnswer.stream()
        .map(
            question -> {
              List<Answer> answers = answerService.answerByQuestionID(question.getQuestionId());
              if (!answers.isEmpty()) {
                boolean flag = false;
                int upvotes = 0;
                int acceptedUpvotes = 0;
                Iterator<Answer> it = answers.iterator();
                while (it.hasNext()) {
                  Answer answer = it.next();
                  if (answer.isAccepted()) {
                    flag = true;
                    acceptedUpvotes = answer.getScore();
                  } else {
                    upvotes = Math.max(upvotes, answer.getScore());
                  }
                }
                if (flag && upvotes > acceptedUpvotes) {
                  return 1;
                }
              }
              return 0;
            })
        .reduce(0, Integer::sum);
  }

  @GetMapping("/with-accepted-answer/better-answer/percentage")
  public double withAcceptedAnswerBetterAnswerPercentageQuery(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Question> withAnswer = questionService.withAnswer(from, end);
    int count =
        withAnswer.stream()
            .map(
                question -> {
                  List<Answer> answers = answerService.answerByQuestionID(question.getQuestionId());
                  if (!answers.isEmpty()) {
                    boolean flag = false;
                    int upvotes = 0;
                    int acceptedUpvotes = 0;
                    Iterator<Answer> it = answers.iterator();
                    while (it.hasNext()) {
                      Answer answer = it.next();
                      if (answer.isAccepted()) {
                        flag = true;
                        acceptedUpvotes = answer.getScore();
                      } else {
                        upvotes = Math.max(upvotes, answer.getScore());
                      }
                    }
                    if (flag && upvotes > acceptedUpvotes) {
                      return 1;
                    }
                  }
                  return 0;
                })
            .reduce(0, Integer::sum);
    return (double) count / questionService.totalQuestion(from, end).size();
  }
}
