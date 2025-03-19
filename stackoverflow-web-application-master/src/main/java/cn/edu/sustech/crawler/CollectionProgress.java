// CollectionProgress.java
package cn.edu.sustech.crawler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.*;

@Data
public class CollectionProgress implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(CollectionProgress.class);
    private static final String PROGRESS_FILE = "collection_progress.json";

    // 总体进度信息
    private int totalQuestions;
    private int noAnswerQuestions;
    private int lastProcessedPage;
    private int totalPages;

    // 详细的采集进度
    private Map<Integer, QuestionProgress> questionProgressMap;
    private Set<Integer> completedQuestionIds;
    private Set<Integer> completedAnswerIds;
    private Set<Integer> completedCommentIds;

    // 批次信息，用于断点续传
    private List<Integer> currentBatchQuestionIds;
    private int currentBatchIndex;

    private LocalDateTime lastUpdateTime;
    private CollectionState state;

    public CollectionProgress() {
        this.questionProgressMap = new HashMap<>();
        this.completedQuestionIds = new HashSet<>();
        this.completedAnswerIds = new HashSet<>();
        this.completedCommentIds = new HashSet<>();
        this.currentBatchQuestionIds = new ArrayList<>();
        this.lastUpdateTime = LocalDateTime.now();
        this.state = CollectionState.NOT_STARTED;
    }

    public static CollectionProgress loadProgress() {
        File file = new File(PROGRESS_FILE);
        if (!file.exists()) {
            return new CollectionProgress();
        }

        try {
            String json = new String(java.nio.file.Files.readAllBytes(file.toPath()));
            CollectionProgress progress = JSON.parseObject(json, CollectionProgress.class);
            logger.info("Loaded progress from {}", progress.getLastUpdateTime());
            return progress;
        } catch (IOException | RuntimeException e) {
            logger.error("Failed to load progress, starting fresh", e);
            return new CollectionProgress();
        }
    }

    // 更新总体统计信息
    public void updateStatistics(int totalQuestions, int noAnswerQuestions, int totalPages) {
        this.totalQuestions = totalQuestions;
        this.noAnswerQuestions = noAnswerQuestions;
        this.totalPages = totalPages;
        saveProgress();
    }

    // 记录问题的采集进度
    public void recordQuestionProgress(int questionId, JSONObject questionData) {
        QuestionProgress progress = new QuestionProgress(questionId, questionData);
        questionProgressMap.put(questionId, progress);
        completedQuestionIds.add(questionId);
        lastUpdateTime = LocalDateTime.now();
        if (completedQuestionIds.size() % 100 == 0) saveProgress();
    }

    // 记录答案的采集进度
    public void recordAnswerProgress(int questionId, int answerId, JSONObject answerData) {
        QuestionProgress progress = questionProgressMap.get(questionId);
        if (progress != null) {
            progress.addAnswer(answerId, answerData);
            completedAnswerIds.add(answerId);
            lastUpdateTime = LocalDateTime.now();
            if (completedAnswerIds.size() % 100 == 0) saveProgress();
        }
    }

    // 记录评论的采集进度
    public void recordCommentProgress(int targetId, boolean isQuestion, JSONObject commentData) {
        int commentId = commentData.getInteger("comment_id");
        if (isQuestion) {
            QuestionProgress progress = questionProgressMap.get(targetId);
            if (progress != null) {
                progress.addQuestionComment(commentId, commentData);
            }
        } else {
            for (QuestionProgress progress : questionProgressMap.values()) {
                if (progress.hasAnswer(targetId)) {
                    progress.addAnswerComment(targetId, commentId, commentData);
                    break;
                }
            }
        }
        completedCommentIds.add(commentId);
        lastUpdateTime = LocalDateTime.now();
        if (completedCommentIds.size() % 100 == 0) saveProgress();
    }

    // 更新批次信息
    public void updateBatch(List<Integer> batchQuestionIds, int batchIndex) {
        this.currentBatchQuestionIds = new ArrayList<>(batchQuestionIds);
        this.currentBatchIndex = batchIndex;
        saveProgress();
    }

    public void saveProgress() {
        try {
            String json = JSON.toJSONString(this);
            File tempFile = new File(PROGRESS_FILE + ".tmp");
            File targetFile = new File(PROGRESS_FILE);

            // 先写入临时文件
            try (FileWriter writer = new FileWriter(tempFile)) {
                writer.write(json);
            }

            // 原子性地替换文件
            if (!tempFile.renameTo(targetFile)) {
                if (targetFile.exists() && !targetFile.delete()) {
                    logger.error("Failed to delete old progress file");
                    return;
                }
                if (!tempFile.renameTo(targetFile)) {
                    logger.error("Failed to rename temp file to progress file");
                }
            }

            //logger.info("Progress saved successfully at {}", lastUpdateTime);
        } catch (IOException e) {
            logger.error("Failed to save progress", e);
        }
    }

    // 获取尚未完成的问题ID列表
    public List<Integer> getIncompleteQuestionIds() {
        //logger.info("Successfully loaded progress from {}", lastUpdateTime);
        List<Integer> incompleteIds = new ArrayList<>();
        for (Map.Entry<Integer, QuestionProgress> entry : questionProgressMap.entrySet()) {
            if (!entry.getValue().isComplete()) {
                incompleteIds.add(entry.getKey());
            }
        }
        return incompleteIds;
    }

    // 检查是否需要继续采集答案
    public boolean needsAnswerCollection(int questionId) {
        QuestionProgress progress = questionProgressMap.get(questionId);
        return progress != null && !progress.hasCollectedAnswers();
    }

    // 检查是否需要继续采集评论
    public boolean needsCommentCollection(int targetId, boolean isQuestion) {
        if (isQuestion) {
            QuestionProgress progress = questionProgressMap.get(targetId);
            return progress != null && !progress.hasCollectedQuestionComments();
        } else {
            for (QuestionProgress progress : questionProgressMap.values()) {
                if (progress.hasAnswer(targetId) && !progress.hasCollectedAnswerComments(targetId)) {
                    return true;
                }
            }
        }
        return false;
    }


    public void setLastProcessedPage(int page) {
        this.lastProcessedPage = page;
        lastUpdateTime = LocalDateTime.now();
        saveProgress();
    }


    // Setters
    public void setState(CollectionState state) {
        this.state = state;
        lastUpdateTime = LocalDateTime.now();
        saveProgress();
    }

    // 内部类：问题进度
    private static class QuestionProgress implements Serializable {
        private final int questionId;
        private final JSONObject questionData;
        private final Map<Integer, JSONObject> answers;
        private final Map<Integer, JSONObject> questionComments;
        private final Map<Integer, Map<Integer, JSONObject>> answerComments;
        private boolean answersCollected; // 是否已经采集了所有答案
        private boolean questionCommentsCollected; // 是否已经采集了该问题的评论
        private boolean answerCommentsCollected; // 是否已经采集了所有答案的评论

        public QuestionProgress() {
            this.questionId = 0;
            this.questionData = new JSONObject();
            this.answers = new HashMap<>();
            this.questionComments = new HashMap<>();
            this.answerComments = new HashMap<>();
            this.answersCollected = false;
            this.questionCommentsCollected = false;
            this.answerCommentsCollected = false;
        }

        public QuestionProgress(int questionId, JSONObject questionData) {
            this.questionId = questionId;
            this.questionData = questionData;
            this.answers = new HashMap<>();
            this.questionComments = new HashMap<>();
            this.answerComments = new HashMap<>();
            this.answersCollected = false;
            this.questionCommentsCollected = false;
            this.answerCommentsCollected = false;
        }

        public void addAnswer(int answerId, JSONObject answerData) {
            answers.put(answerId, answerData);
        }

        public void addQuestionComment(int commentId, JSONObject commentData) {
            questionComments.put(commentId, commentData);
        }

        public void addAnswerComment(int answerId, int commentId, JSONObject commentData) {
            answerComments.computeIfAbsent(answerId, k -> new HashMap<>())
                    .put(commentId, commentData);
        }

        public boolean hasAnswer(int answerId) {
            return answers.containsKey(answerId);
        }

        public boolean isComplete() {
            return answersCollected && questionCommentsCollected && answerCommentsCollected;
        }

        public boolean hasCollectedAnswers() {
            return answersCollected;
        }

        public boolean hasCollectedQuestionComments() {
            return questionCommentsCollected;
        }

        public boolean hasCollectedAnswerComments(int answerId) {
            return answerCommentsCollected && answers.containsKey(answerId);
        }
    }
}