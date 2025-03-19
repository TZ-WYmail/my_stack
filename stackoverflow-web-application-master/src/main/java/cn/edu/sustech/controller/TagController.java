package cn.edu.sustech.controller;

import cn.edu.sustech.entity.Question;
import cn.edu.sustech.entity.Tag;
import cn.edu.sustech.entity.TagConnect;
import cn.edu.sustech.service.QuestionService;
import cn.edu.sustech.service.TagService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/tag")
public class TagController {

  private static final boolean REMOVE_JAVA = true;

  @Autowired private TagService tagService;
  @Autowired private QuestionService questionService;

  @GetMapping("/single-tag/all/name")
  public List<Tag> allTagsQuery() {
    return tagService.allTags();
  }

  @GetMapping("/group-tag/all/name")
  public List<String> allTagGroupsQuery() {
    List<Map<String, Object>> tagGroups = tagService.tagGroups();
    return tagGroups.stream()
        .map(tagGroup -> (String) tagGroup.get("tag_group"))
        .distinct()
        .collect(Collectors.toList());
  }

  @GetMapping("/single-tag/all/count")
  public List<Map<String, Object>> allTagCountsQuery() {
    return tagService.tagCounts();
  }

  private static List<List<String>> getSubListsOfSize(List<String> list, int size) {
    List<String> nowList = list.stream().distinct().sorted().toList();
    List<List<String>> result = new ArrayList<>();
    for (int i = 0; i < (1 << nowList.size()); i++) {
      if (Integer.bitCount(i) == size) {
        List<String> subList = new ArrayList<>();
        for (int j = 0; j < nowList.size(); j++) {
          if ((i & (1 << j)) != 0) {
            subList.add(nowList.get(j));
          }
        }
        result.add(subList);
      }
    }
    return result;
  }

  @GetMapping("/group-tag/all/count")
  public List<Map<String, Object>> allTagGroupCountsQuery(@RequestParam("size") int size) {
    List<Map<String, Object>> tagGroups = tagService.tagGroups();
    Map<String, Integer> map = new HashMap<>();
    tagGroups.forEach(
        tagGroup -> {
          List<String> tags = Arrays.asList(((String) tagGroup.get("tags")).split(","));
          List<List<String>> subListsOfSize = getSubListsOfSize(tags, size);
          subListsOfSize.forEach(
              subList -> {
                String key = subList.stream().sorted().collect(Collectors.joining(","));
                map.merge(key, 1, Integer::sum);
              });
        });
    List<Map<String, Object>> result = new ArrayList<>();
    map.entrySet().stream()
        .sorted((e1, e2) -> -e1.getValue().compareTo(e2.getValue()))
        .forEach(
            e -> {
              result.add(Map.of("tags", e.getKey(), "count", e.getValue()));
            });
    return result;
  }

  @GetMapping("/single-tag/count")
  public List<Map<String, Object>> getSingeTagCount(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Question> total = questionService.totalQuestion(from, end);
    Map<String, Integer> map = new HashMap<>();
    total.forEach(
        question -> {
          List<TagConnect> tags = tagService.tagsByQuestionId(question.getQuestionId());
          tags.forEach(
              tag -> {
                String tagName = tag.getTagName();
                if (REMOVE_JAVA && tagName.equals("java")) {
                  return;
                }
                map.merge(tagName, 1, Integer::sum);
              });
        });
    List<Map<String, Object>> result = new ArrayList<>();
    map.entrySet().stream()
        .sorted((e1, e2) -> -e1.getValue().compareTo(e2.getValue()))
        .forEach(
            e -> {
              result.add(Map.of("tag", e.getKey(), "count", e.getValue()));
            });
    return result;
  }

  @GetMapping("/group-tag/count")
  public List<Map<String, Object>> allTagGroupCountsQuery(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end,
      @RequestParam("size") int size) {
    List<Question> total = questionService.totalQuestion(from, end);
    Map<String, Integer> map = new HashMap<>();
    total.forEach(
        question -> {
          List<TagConnect> tags = tagService.tagsByQuestionId(question.getQuestionId());
          List<String> tagNames =
              new ArrayList<>(tags.stream().map(TagConnect::getTagName).toList());
          if (REMOVE_JAVA) {
            tagNames.remove("java");
          }
          List<List<String>> subListsOfSize = getSubListsOfSize(tagNames, size);
          subListsOfSize.forEach(
              subList -> {
                String key = subList.stream().sorted().collect(Collectors.joining(","));
                map.merge(key, 1, Integer::sum);
              });
        });
    List<Map<String, Object>> result = new ArrayList<>();
    map.entrySet().stream()
        .sorted((e1, e2) -> -e1.getValue().compareTo(e2.getValue()))
        .forEach(
            e -> {
              result.add(Map.of("tags", e.getKey(), "count", e.getValue()));
            });
    return result;
  }

  @GetMapping("/single-tag/upvote")
  public List<Map<String, Object>> getSingeTagUpvote(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Question> total = questionService.totalQuestion(from, end);
    Map<String, Integer> map = new HashMap<>();
    total.forEach(
        question -> {
          List<TagConnect> tags = tagService.tagsByQuestionId(question.getQuestionId());
          tags.forEach(
              tag -> {
                String tagName = tag.getTagName();
                if (REMOVE_JAVA && tagName.equals("java")) {
                  return;
                }
                map.merge(tagName, question.getScore(), Integer::sum);
              });
        });
    List<Map<String, Object>> result = new ArrayList<>();
    map.entrySet().stream()
        .sorted((e1, e2) -> -e1.getValue().compareTo(e2.getValue()))
        .forEach(
            e -> {
              result.add(Map.of("tag", e.getKey(), "count", e.getValue()));
            });
    return result;
  }

  @GetMapping("/group-tag/upvote")
  public List<Map<String, Object>> getGroupTagUpvote(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end,
      @RequestParam("size") int size) {
    List<Question> total = questionService.totalQuestion(from, end);
    Map<String, Integer> map = new HashMap<>();
    total.forEach(
        question -> {
          List<TagConnect> tags = tagService.tagsByQuestionId(question.getQuestionId());
          List<String> tagNames =
              new ArrayList<>(tags.stream().map(TagConnect::getTagName).toList());
          if (REMOVE_JAVA) {
            tagNames.remove("java");
          }
          List<List<String>> subListsOfSize = getSubListsOfSize(tagNames, size);
          subListsOfSize.forEach(
              subList -> {
                String key = subList.stream().sorted().collect(Collectors.joining(","));
                map.merge(key, question.getScore(), Integer::sum);
              });
        });
    List<Map<String, Object>> result = new ArrayList<>();
    map.entrySet().stream()
        .sorted((e1, e2) -> -e1.getValue().compareTo(e2.getValue()))
        .forEach(
            e -> {
              result.add(Map.of("tags", e.getKey(), "count", e.getValue()));
            });
    return result;
  }

  @GetMapping("/single-tag/view")
  public List<Map<String, Object>> getSingeTagView(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    List<Question> total = questionService.totalQuestion(from, end);
    Map<String, Integer> map = new HashMap<>();
    total.forEach(
        question -> {
          List<TagConnect> tags = tagService.tagsByQuestionId(question.getQuestionId());
          tags.forEach(
              tag -> {
                String tagName = tag.getTagName();
                if (REMOVE_JAVA && tagName.equals("java")) {
                  return;
                }
                map.merge(tagName, question.getViewCount(), Integer::sum);
              });
        });
    List<Map<String, Object>> result = new ArrayList<>();
    map.entrySet().stream()
        .sorted((e1, e2) -> -e1.getValue().compareTo(e2.getValue()))
        .forEach(
            e -> {
              result.add(Map.of("tag", e.getKey(), "count", e.getValue()));
            });
    return result;
  }

  @GetMapping("/group-tag/view")
  public List<Map<String, Object>> getGroupTagView(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end,
      @RequestParam("size") int size) {
    List<Question> total = questionService.totalQuestion(from, end);
    Map<String, Integer> map = new HashMap<>();
    total.forEach(
        question -> {
          List<TagConnect> tags = tagService.tagsByQuestionId(question.getQuestionId());
          List<String> tagNames =
              new ArrayList<>(tags.stream().map(TagConnect::getTagName).toList());
          if (REMOVE_JAVA) {
            tagNames.remove("java");
          }
          List<List<String>> subListsOfSize = getSubListsOfSize(tagNames, size);
          subListsOfSize.forEach(
              subList -> {
                String key = subList.stream().sorted().collect(Collectors.joining(","));
                map.merge(key, question.getViewCount(), Integer::sum);
              });
        });
    List<Map<String, Object>> result = new ArrayList<>();
    map.entrySet().stream()
        .sorted((e1, e2) -> -e1.getValue().compareTo(e2.getValue()))
        .forEach(
            e -> {
              result.add(Map.of("tags", e.getKey(), "count", e.getValue()));
            });
    return result;
  }
}
