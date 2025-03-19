package cn.edu.sustech.controller;

import cn.edu.sustech.service.ApiService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/api")
public class ApiController {

  @Autowired private ApiService apiService;

  @GetMapping("/count")
  public List<Map<String, Object>> getApiCount(
      @RequestParam("from") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date from,
      @RequestParam("end") @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss") Date end) {
    Map<String, Integer> result = apiService.getApiCount(from, end);
    List<Map<String, Object>> resultList =
        result.entrySet().stream()
            .sorted(Comparator.comparing(Map.Entry<String, Integer>::getValue).reversed())
            .map(
                entry -> {
                  Map<String, Object> map = new HashMap<>();
                  map.put("api", entry.getKey());
                  map.put("count", entry.getValue());
                  return map;
                })
            .collect(Collectors.toList());

    return resultList;
  }
}
