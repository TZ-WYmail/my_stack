package cn.edu.sustech.crawler;

import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DataCollector {
    private static final Logger logger = LoggerFactory.getLogger(DataCollector.class);

    private final StackOverflowService stackOverflowService;
    private final DatabaseService databaseService;
    private final CollectionProgress progress;

    // 存储采集到的数据
    private final List<JSONObject> questionList = new ArrayList<>();
    private final List<JSONObject> answerList = new ArrayList<>();
    private final List<JSONObject> commentList = new ArrayList<>();

    // 配置参数
    private final int pageSize;
    private final int pageStep;

    // 统计信息
    private int totalQuestions;
    private int noAnswerQuestions;
    private Timestamp lastRefreshTime;

    public DataCollector(DatabaseService databaseService, int pageSize, int pageStep) {
        this.databaseService = databaseService;
        this.pageSize = pageSize;
        this.pageStep = pageStep;
        this.stackOverflowService = new StackOverflowService(pageSize);
        this.progress = CollectionProgress.loadProgress();
        refresh();
    }

    public void refresh() {
        try {
            JSONObject questionStats = stackOverflowService.getQuestionStats();
            JSONObject noAnswerStats = stackOverflowService.getNoAnswerStats();

            this.totalQuestions = questionStats.getInteger("total");
            this.noAnswerQuestions = noAnswerStats.getInteger("total");
            this.lastRefreshTime = new Timestamp(System.currentTimeMillis());

        } catch (ApiException e) {
            logger.error("Failed to refresh statistics", e);
            throw e;
        }
    }

    public void collectData() {
        logger.info("Starting data collection");
        try {
            // 更新总体统计信息
            JSONObject stats = stackOverflowService.getQuestionStats();
            progress.updateStatistics(
                    stats.getInteger("total"),
                    stackOverflowService.getNoAnswerStats().getInteger("total"),
                    calculateTotalPages(stats.getInteger("total"))
            );

            // 根据状态选择开始方式
            if (progress.getState() == CollectionState.NOT_STARTED ||
                    progress.getState() == CollectionState.FAILED) {
                startNewCollection();
            } else {
                resumeCollection();
            }

        } catch (Exception e) {
            logger.error("Collection failed", e);
            progress.setState(CollectionState.FAILED);
            throw e;
        }
    }

    private void startNewCollection() {
        logger.info("Starting new collection");
        progress.setState(CollectionState.COLLECTING_QUESTIONS);
        collectQuestions();
        collectAnswers();
        collectComments();
        saveToDatabase();
    }

    private void resumeCollection() {
        logger.info("Resuming collection from state: {}", progress.getState());
        CollectionState currentState = progress.getState();

        switch (currentState) {
            case COLLECTING_QUESTIONS:
                collectQuestions();
                collectAnswers();
                collectComments();
                saveToDatabase();
                break;

            case COLLECTING_ANSWERS:
                collectAnswers();
                collectComments();
                saveToDatabase();
                break;

            case COLLECTING_QUESTION_COMMENTS:
            case COLLECTING_ANSWER_COMMENTS:
                collectComments();
                saveToDatabase();
                break;

            case SAVING_TO_DATABASE:
                saveToDatabase();
                break;

            default:
                logger.warn("Unexpected state: {}, starting fresh", currentState);
                startNewCollection();
        }
    }

    private void collectQuestions() {
        progress.setState(CollectionState.COLLECTING_QUESTIONS);
        int pageTotal = totalQuestions / pageSize;
        int startPage = progress.getLastProcessedPage() + 1;

        for (int page = startPage; page <= pageTotal; page += pageStep) {
            logger.info("Collecting questions - Progress: {}%", (int) (100.0 * page / pageTotal));
            try {
                List<JSONObject> questions = stackOverflowService.getQuestions(page);
                for (JSONObject question : questions) {
                    int questionId = question.getInteger("question_id");
                    if (!questionList.contains(question)) {
                        questionList.add(question);
                        progress.recordQuestionProgress(questionId, question);
                    }
                }
                progress.setLastProcessedPage(page);
            } catch (Exception e) {
                logger.error("Error collecting questions at page {}", page, e);
                throw e;
            }
        }
        logger.info("Questions collection completed, total questions: {}", questionList.size());
    }

    private void collectAnswers() {
        progress.setState(CollectionState.COLLECTING_ANSWERS);
        List<Integer> questionIds = new ArrayList<>();

        for (JSONObject question : questionList) {
            int questionId = question.getInteger("question_id");
            questionIds.add(questionId);

            if (questionIds.size() == 100) {
                logger.info("Collecting answers for question: {}, progress: {}%", question.getInteger("question_id"),
                        (100.0 * questionList.indexOf(question) / questionList.size()));
                processAnswerBatch(questionIds);
                questionIds.clear();
            }
        }

        if (!questionIds.isEmpty()) {
            processAnswerBatch(questionIds);
        }
        logger.info("Answers collection completed, total answers: {}", answerList.size());
    }

    private void processAnswerBatch(List<Integer> questionIds) {
        try {
            List<JSONObject> answers = stackOverflowService.getAnswers(questionIds);
            for (JSONObject answer : answers) {
                int answerId = answer.getInteger("answer_id");
                int questionId = answer.getInteger("question_id");
                if (!answerList.contains(answer)) {
                    answerList.add(answer);
                    progress.recordAnswerProgress(questionId, answerId, answer);
                }
            }
        } catch (Exception e) {
            logger.error("Error collecting answers for questions: {}", questionIds, e);
            throw e;
        }
    }

    private void collectComments() {
        // Collect question comments
        progress.setState(CollectionState.COLLECTING_QUESTION_COMMENTS);
        List<Integer> questionIds = questionList.stream()
                .map(q -> q.getInteger("question_id"))
                .collect(Collectors.toList());

        for (int i = 0; i < questionIds.size(); i += 100) {
            List<Integer> batch = questionIds.subList(i, Math.min(i + 100, questionIds.size()));
            List<JSONObject> comments = stackOverflowService.getComments("question", batch);
            for (JSONObject comment : comments) {
                if (!commentList.contains(comment)) {
                    commentList.add(comment);
                    progress.recordCommentProgress(
                            comment.getInteger("post_id"),
                            true,
                            comment
                    );
                }
            }
            logger.info("Collecting question comments - Progress: {}%",  (100.0 * i / questionIds.size()));
        }

        // Collect answer comments
        progress.setState(CollectionState.COLLECTING_ANSWER_COMMENTS);
        List<Integer> answerIds = answerList.stream()
                .map(a -> a.getInteger("answer_id"))
                .collect(Collectors.toList());

        for (int i = 0; i < answerIds.size(); i += 100) {
            List<Integer> batch = answerIds.subList(i, Math.min(i + 100, answerIds.size()));
            List<JSONObject> comments = stackOverflowService.getComments("answer", batch);
            for (JSONObject comment : comments) {
                if (!commentList.contains(comment)) {
                    commentList.add(comment);
                    progress.recordCommentProgress(
                            comment.getInteger("post_id"),
                            false,
                            comment
                    );
                }
            }
            logger.info("Collecting answer comments - Progress: {}%",  (100.0 * i / questionIds.size()));
        }
        logger.info("Comments collection completed, total comments: {}", commentList.size());
    }

    private void saveToDatabase() {
        progress.setState(CollectionState.SAVING_TO_DATABASE);
        logger.info("Saving data to database");

        try {
            databaseService.saveToDatabase(questionList, answerList, commentList);
            progress.setState(CollectionState.COMPLETED);
            logger.info("Data collection completed successfully");
        } catch (Exception e) {
            logger.error("Database save failed", e);
            throw new ApiException("Failed to save to database", e);
        }
    }

    private int calculateTotalPages(Integer total) {
        return (int) Math.ceil((double) total / pageSize);
    }

    public double getNoAnswerPercent() {
        return (double) noAnswerQuestions / totalQuestions;
    }
}