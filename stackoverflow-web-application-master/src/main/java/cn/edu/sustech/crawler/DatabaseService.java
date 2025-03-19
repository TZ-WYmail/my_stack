package cn.edu.sustech.crawler;

import com.alibaba.fastjson.JSONObject;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;
import java.util.Map;

public class DatabaseService implements AutoCloseable {
    private static final int BATCH_SIZE = 1000;
    private final Logger logger = LoggerFactory.getLogger(DatabaseService.class);
    private final StanfordCoreNLPService stanfordCoreNLPService;
    private HikariDataSource dataSource;

    // 预编译SQL语句的缓存
    private PreparedStatement questionStmt;
    private PreparedStatement answerStmt;
    private PreparedStatement commentStmt;
    private PreparedStatement ownerStmt;
    private PreparedStatement tagStmt;
    private PreparedStatement apiStmt;
    private PreparedStatement tagQuestionStmt;
    private PreparedStatement apiQuestionStmt;
    private PreparedStatement apiAnswerStmt;
    private PreparedStatement apiCommentStmt;

    // 批处理计数器
    private int questionBatchCount = 0;
    private int answerBatchCount = 0;
    private int commentBatchCount = 0;

    public DatabaseService(String host, int port, String user, String password, String database) {
        setupConnectionPool(host, port, user, password, database);
        stanfordCoreNLPService = new StanfordCoreNLPService();
    }

    private void setupConnectionPool(String host, int port, String user, String password, String database) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + host + ":" + port + "/" + database);
        config.setUsername(user);
        config.setPassword(password);

        // 连接池配置
        config.setMaximumPoolSize(10);
        config.setMinimumIdle(5);
        config.setIdleTimeout(300000);
        config.setConnectionTimeout(20000);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        dataSource = new HikariDataSource(config);
    }

    private void prepareStatements(Connection conn) throws SQLException {
        questionStmt = conn.prepareStatement(
                "INSERT INTO question VALUES (?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING"
        );
        answerStmt = conn.prepareStatement(
                "INSERT INTO answer VALUES (?,?,?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING"
        );
        commentStmt = conn.prepareStatement(
                "INSERT INTO comment VALUES (?,?,?,?,?,?,?,?) ON CONFLICT DO NOTHING"
        );
        ownerStmt = conn.prepareStatement(
                "INSERT INTO owner VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING"
        );
        tagStmt = conn.prepareStatement(
                "INSERT INTO tag VALUES (?) ON CONFLICT DO NOTHING"
        );
        apiStmt = conn.prepareStatement(
                "INSERT INTO api VALUES (?) ON CONFLICT DO NOTHING"
        );
        tagQuestionStmt = conn.prepareStatement(
                "INSERT INTO connection_tag_and_question VALUES (?,?) ON CONFLICT DO NOTHING"
        );
        apiQuestionStmt = conn.prepareStatement(
                "INSERT INTO connection_question_and_api VALUES (?,?,?) ON CONFLICT DO NOTHING"
        );
        apiAnswerStmt = conn.prepareStatement(
                "INSERT INTO connection_answer_and_api VALUES (?,?,?) ON CONFLICT DO NOTHING"
        );
        apiCommentStmt = conn.prepareStatement(
                "INSERT INTO connection_comment_and_api VALUES (?,?,?) ON CONFLICT DO NOTHING"
        );
    }

    public void batchInsertQuestionRecord(List<JSONObject> questions) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            prepareStatements(conn);

            for (JSONObject question : questions) {
                // 插入问题相关数据
                User owner = getUser(question);
                addQuestionBatch(question, owner);
                addOwnerBatch(owner);

                // 处理标签
                for (Object tag : question.getJSONArray("tags")) {
                    addTagBatch((String) tag);
                    addTagQuestionBatch((String) tag, question.getInteger("question_id"));
                }

                // 处理API
                Map<String, Integer> apiCount = stanfordCoreNLPService.getAllJavaAPI(question.getString("body"));
                for (Map.Entry<String, Integer> entry : apiCount.entrySet()) {
                    addApiBatch(entry.getKey());
                    addApiQuestionBatch(question.getInteger("question_id"), entry.getKey(), entry.getValue());
                }

                questionBatchCount++;
                if (questionBatchCount >= BATCH_SIZE) {
                    executeBatch(conn);
                }
            }

            // 执行剩余的批处理
            executeBatch(conn);
            conn.commit();
        }
    }

    private void addQuestionBatch(JSONObject question, User owner) throws SQLException {
        questionStmt.setInt(1, question.getInteger("question_id"));
        questionStmt.setInt(2, question.getInteger("score"));
        questionStmt.setString(3, question.getString("link"));
        questionStmt.setInt(4, question.getInteger("answer_count"));
        questionStmt.setInt(5, question.getInteger("view_count"));
        questionStmt.setString(6, question.getString("content_license"));
        questionStmt.setString(7, question.getString("title"));
        questionStmt.setTimestamp(8, convertDate(question.getInteger("last_activity_date")));
        questionStmt.setTimestamp(9, convertDate(question.getInteger("last_edit_date")));
        questionStmt.setTimestamp(10, convertDate(question.getInteger("creation_date")));
        questionStmt.setInt(11, owner.getAccountId());
        questionStmt.setString(12, question.getString("body"));
        questionStmt.addBatch();
    }

    private void executeBatch(Connection conn) throws SQLException {
        questionStmt.executeBatch();
        ownerStmt.executeBatch();
        tagStmt.executeBatch();
        apiStmt.executeBatch();
        tagQuestionStmt.executeBatch();
        apiQuestionStmt.executeBatch();

        questionBatchCount = 0;
        conn.commit();
    }

    public void saveToDatabase(List<JSONObject> questions, List<JSONObject> answers, List<JSONObject> comments) {
        try {
            batchInsertQuestionRecord(questions);
            batchInsertAnswerRecord(answers);
            batchInsertCommentRecord(comments);
            insertUpdateTime();
            logger.info("Data successfully saved to database");
        } catch (SQLException e) {
            logger.error("Failed to save data to database", e);
            throw new RuntimeException("Database operation failed", e);
        }
    }
    private void addOwnerBatch(User owner) throws SQLException {
        ownerStmt.setInt(1, owner.getAccountId());
        ownerStmt.setInt(2, owner.getUserId());
        ownerStmt.setString(3, owner.getProfileImage());
        ownerStmt.setString(4, owner.getLink());
        ownerStmt.setString(5, owner.getUserType());
        ownerStmt.setString(6, owner.getAccountId() == -1 ? "does_not_exist" : owner.getDisplayName());
        ownerStmt.setInt(7, owner.getReputation());
        ownerStmt.addBatch();
    }

    private void addTagBatch(String tagName) throws SQLException {
        tagStmt.setString(1, tagName);
        tagStmt.addBatch();
    }

    private void addApiBatch(String apiName) throws SQLException {
        apiStmt.setString(1, apiName);
        apiStmt.addBatch();
    }

    private void addTagQuestionBatch(String tagName, int questionId) throws SQLException {
        tagQuestionStmt.setString(1, tagName);
        tagQuestionStmt.setInt(2, questionId);
        tagQuestionStmt.addBatch();
    }

    private void addApiQuestionBatch(int questionId, String apiName, int count) throws SQLException {
        apiQuestionStmt.setInt(1, questionId);
        apiQuestionStmt.setString(2, apiName);
        apiQuestionStmt.setInt(3, count);
        apiQuestionStmt.addBatch();
    }

    private void addApiAnswerBatch(int answerId, String apiName, int count) throws SQLException {
        apiAnswerStmt.setInt(1, answerId);
        apiAnswerStmt.setString(2, apiName);
        apiAnswerStmt.setInt(3, count);
        apiAnswerStmt.addBatch();
    }

    private void addApiCommentBatch(int commentId, String apiName, int count) throws SQLException {
        apiCommentStmt.setInt(1, commentId);
        apiCommentStmt.setString(2, apiName);
        apiCommentStmt.setInt(3, count);
        apiCommentStmt.addBatch();
    }

    public void batchInsertAnswerRecord(List<JSONObject> answers) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            prepareStatements(conn);

            for (JSONObject answer : answers) {
                User owner = getUser(answer);
                addAnswerBatch(answer, owner);
                addOwnerBatch(owner);

                // 处理API
                Map<String, Integer> apiCount = stanfordCoreNLPService.getAllJavaAPI(answer.getString("body"));
                for (Map.Entry<String, Integer> entry : apiCount.entrySet()) {
                    addApiBatch(entry.getKey());
                    addApiAnswerBatch(answer.getInteger("answer_id"), entry.getKey(), entry.getValue());
                }

                answerBatchCount++;
                if (answerBatchCount >= BATCH_SIZE) {
                    executeAnswerBatch(conn);
                }
            }

            // 执行剩余的批处理
            executeAnswerBatch(conn);
            conn.commit();
        }
    }

    private void addAnswerBatch(JSONObject answer, User owner) throws SQLException {
        answerStmt.setInt(1, answer.getInteger("answer_id"));
        answerStmt.setTimestamp(2, convertDate(answer.getInteger("last_activity_date")));
        answerStmt.setTimestamp(3, convertDate(answer.getInteger("last_edit_date")));
        answerStmt.setTimestamp(4, convertDate(answer.getInteger("creation_date")));
        answerStmt.setInt(5, answer.getInteger("score"));
        answerStmt.setBoolean(6, answer.getBoolean("is_accepted"));
        answerStmt.setString(7, answer.getString("content_license"));
        answerStmt.setInt(8, answer.getInteger("question_id"));
        answerStmt.setString(9, answer.getString("body"));
        answerStmt.setInt(10, owner.getAccountId());
        answerStmt.addBatch();
    }

    private void executeAnswerBatch(Connection conn) throws SQLException {
        answerStmt.executeBatch();
        ownerStmt.executeBatch();
        apiStmt.executeBatch();
        apiAnswerStmt.executeBatch();

        answerBatchCount = 0;
        conn.commit();
    }

    public void batchInsertCommentRecord(List<JSONObject> comments) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);
            prepareStatements(conn);

            for (JSONObject comment : comments) {
                User owner = getUser(comment);
                addCommentBatch(comment, owner);
                addOwnerBatch(owner);

                // 处理API
                Map<String, Integer> apiCount = stanfordCoreNLPService.getAllJavaAPI(comment.getString("body"));
                for (Map.Entry<String, Integer> entry : apiCount.entrySet()) {
                    addApiBatch(entry.getKey());
                    addApiCommentBatch(comment.getInteger("comment_id"), entry.getKey(), entry.getValue());
                }

                commentBatchCount++;
                if (commentBatchCount >= BATCH_SIZE) {
                    executeCommentBatch(conn);
                }
            }

            // 执行剩余的批处理
            executeCommentBatch(conn);
            conn.commit();
        }
    }

    private void addCommentBatch(JSONObject comment, User owner) throws SQLException {
        commentStmt.setInt(1, comment.getInteger("comment_id"));
        commentStmt.setBoolean(2, comment.getBoolean("edited"));
        commentStmt.setInt(3, comment.getInteger("post_id"));
        commentStmt.setString(4, comment.getString("body"));
        commentStmt.setTimestamp(5, convertDate(comment.getInteger("creation_date")));
        commentStmt.setInt(6, comment.getInteger("score"));
        commentStmt.setString(7, comment.getString("content_license"));
        commentStmt.setInt(8, owner.getAccountId());
        commentStmt.addBatch();
    }

    private void executeCommentBatch(Connection conn) throws SQLException {
        commentStmt.executeBatch();
        ownerStmt.executeBatch();
        apiStmt.executeBatch();
        apiCommentStmt.executeBatch();

        commentBatchCount = 0;
        conn.commit();
    }

    private static User getUser(JSONObject json) {
        JSONObject ownerJson = json.getJSONObject("owner");
        return new User(
                ownerJson.getString("profile_image"),
                ownerJson.getInteger("account_id") == null ? -1 : ownerJson.getInteger("account_id"),
                ownerJson.getString("user_type"),
                ownerJson.getInteger("user_id") == null ? -1 : ownerJson.getInteger("user_id"),
                ownerJson.getString("link") == null ? "" : ownerJson.getString("link"),
                ownerJson.getString("display_name"),
                ownerJson.getInteger("reputation") == null ? -1 : ownerJson.getInteger("reputation")
        );
    }

    private Timestamp convertDate(Integer date) {
        return date == null ? null : new Timestamp(date * 1000L);
    }

    public void insertUpdateTime() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("INSERT INTO last_update VALUES (?)")) {
            stmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            stmt.executeUpdate();
        }
    }

    public boolean isConnected() {
        return dataSource != null && !dataSource.isClosed();
    }

    public void createTables() throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            // 保持原有的建表SQL不变
            stmt.executeUpdate("""
                     create table if not exists "owner" (
                        account_id int primary key,
                        user_id int not null,
                        profile_image text,
                        link text not null,
                        user_type text not null,
                        display_name text not null,
                        reputation int not null
                      );
                      create table if not exists question (
                          question_id int primary key,
                          score int not null,
                          link text not null,
                          answer_count int not null,
                          view_count int not null,
                          content_license text,
                          title text not null,
                          last_activity_date timestamp not null,
                          last_edit_date timestamp,
                          creation_date timestamp not null,
                          account_id int not null,
                          body text not null
                          -- foreign key (account_id) references owner(account_id)
                      );
                      create table if not exists answer(
                          answer_id int primary key,
                          last_activity_date timestamp not null,
                          last_edit_date timestamp,
                          creation_date timestamp not null,
                          score int not null,
                          is_accepted bool not null,
                          content_license text,
                          question_id int not null,
                          body text not null,
                          account_id int not null
                          -- foreign key (question_id) references question(question_id),
                          -- foreign key (account_id) references owner(account_id)
                      );
                      create table if not exists comment (
                          comment_id int primary key,
                          edited bool not null,
                          post_id int not null,
                          body text not null,
                          creation_date timestamp not null,
                          score int not null,
                          content_license text,
                          account_id int not null
                          -- foreign key (account_id) references owner(account_id)
                          -- foreign key (post_id) references answer(answer_id)
                     );
                      create table if not exists tag (
                          tag_name text primary key
                      );
                      create table if not exists api(
                          api_name text primary key
                      );
                      create table if not exists connection_tag_and_question (
                          tag_name text not null,
                          question_id int not null
                         -- foreign key (tag_name) references tag (tag_name)
                          -- foreign key (tag_name) references tag (tag_name)
                      -- foreign key (question_id) references question(question_id)
                         );
                      create table if not exists connection_question_and_api (
                          question_id int not null,
                          api_name text not null,
                          count int not null
                          -- foreign key (question_id) references question(question_id),
                          -- foreign key (question_id) references question(question_id)
                      -- foreign key (api_name) references api(api_name)
                    );
                      create table if not exists connection_answer_and_api(
                          answer_id int not null,
                          api_name text not null,
                          count int not null
                         -- foreign key (answer_id) references answer(answer_id)
                          -- foreign key (answer_id) references answer(answer_id)
                      -- foreign key (api_name) references api(api_name)
                      );
                      create table if not exists connection_comment_and_api(
                          comment_id int not null,
                          api_name text not null,
                          count int not null
                          -- foreign key (comment_id) references comment(comment_id)
                          -- foreign key (api_name) references api(api_name)
                      );
                      create table if not exists last_update(
                          last_update_time timestamp not null
                      );
                     """);
        }
    }

    @Override
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    public <T> T executeInTransaction(SqlFunction<T> function) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            try {
                conn.setAutoCommit(false);
                T result = function.apply(conn);
                conn.commit();
                return result;
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
    }

    @FunctionalInterface
    interface SqlFunction<T> {
        T apply(Connection connection) throws SQLException;
    }

    public void executeWithoutForeignKeyCheck(Runnable action) throws SQLException {
        try (Connection conn = dataSource.getConnection()) {
            try {
                conn.setAutoCommit(false);

                // 禁用外键检查
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("SET session_replication_role = replica;");
                    logger.info("Disabled foreign key check");
                }

                // 执行操作
                action.run();

                // 重新启用外键检查
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("SET session_replication_role = DEFAULT;");
                    logger.info("Enabled foreign key check");
                }

                conn.commit();
            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
    }
}

