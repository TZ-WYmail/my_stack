package cn.edu.sustech.crawler;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class StackOverflowService {
    private static final Logger logger = LoggerFactory.getLogger(StackOverflowService.class);
    private final ApiClient apiClient;
    private final int pageSize;

    public StackOverflowService(int pageSize) {
        this.apiClient = new ApiClient();
        this.pageSize = pageSize;
    }

    public JSONObject getQuestionStats() {
        String params = "filter=total&tagged=java";
        return apiClient.executeRequest("questions", params);
    }

    public JSONObject getNoAnswerStats() {
        String params = "filter=total&tagged=java";
        return apiClient.executeRequest("questions/no-answers", params);
    }

    public List<JSONObject> getQuestions(int page) {
        String params = String.format("page=%d&pagesize=%d&order=desc&sort=activity&tagged=java&filter=withbody",
                page, pageSize);
        JSONObject response = apiClient.executeRequest("questions", params);
        return extractItems(response);
    }

    public List<JSONObject> getAnswers(List<Integer> questionIds) {
        if (questionIds.isEmpty()) {
            return new ArrayList<>();
        }

        // 直到has_more为false
        List<JSONObject> answers = new ArrayList<>();
        int page = 1;
        JSONObject response;
        do {
            String ids = String.join(";", questionIds.stream().map(String::valueOf).toArray(String[]::new));
            String params = String.format("page=%d&pagesize=%d&filter=withbody&order=desc&sort=activity",
                    page++, pageSize);
            response = apiClient.executeRequest("questions/" + ids + "/answers", params);
            answers.addAll(extractItems(response));
        } while (response.getBoolean("has_more"));

        return answers;
    }

    public List<JSONObject> getComments(String type, List<Integer> ids) {
        if (ids.isEmpty()) {
            return new ArrayList<>();
        }

        List<JSONObject> comments = new ArrayList<>();
        int page = 1;
        JSONObject response;
        do {
            String idsStr = String.join(";", ids.stream().map(String::valueOf).toArray(String[]::new));
            String endpoint = type.equals("question") ? "questions/" : "answers/";
            String params = String.format("page=%d&pagesize=%d&filter=withbody&order=desc&sort=creation",
                    page++, pageSize);
            response = apiClient.executeRequest(endpoint + idsStr + "/comments", params);
            comments.addAll(extractItems(response));
        } while (response.getBoolean("has_more"));

        return comments;
    }

    private List<JSONObject> extractItems(JSONObject response) {
        List<JSONObject> items = new ArrayList<>();
        JSONArray itemsArray = response.getJSONArray("items");

        if (itemsArray != null) {
            for (int i = 0; i < itemsArray.size(); i++) {
                items.add(itemsArray.getJSONObject(i));
            }
        }

        return items;
    }
}