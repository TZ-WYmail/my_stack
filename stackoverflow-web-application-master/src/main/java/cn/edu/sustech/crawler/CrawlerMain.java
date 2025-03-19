package cn.edu.sustech.crawler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class CrawlerMain {

    private static final String SQL_HOST = "localhost";
    private static final String SQL_USER = "postgres";
    private static final String SQL_PASSWORD = "123456";
    private static final String SQL_DATABASE = "stackoverflow";
    private static final int SQL_PORT = 5432;

    private static final int PAGE_SIZE = 100;
    private static final int PAGE_STEP = 100;

    private static final Logger logger = LoggerFactory.getLogger(CrawlerMain.class);

    public static void main(String[] args) throws SQLException, IOException {
        // 使用 try-with-resources 自动管理资源
        try (DatabaseService databaseService = new DatabaseService(
                SQL_HOST, SQL_PORT, SQL_USER, SQL_PASSWORD, SQL_DATABASE)) {

            DataCollector dataCollector = new DataCollector(databaseService, PAGE_SIZE, PAGE_STEP);
            databaseService.createTables();

            // 在禁用外键检查的状态下执行数据收集
            databaseService.executeWithoutForeignKeyCheck(() -> {
                try {
                    dataCollector.collectData();
                } catch (Exception e) {
                    throw new RuntimeException("Failed to collect data", e);
                }
            });

        } catch (Exception e) {
            logger.error("Application failed", e);
            System.exit(1);
        }
    }
}
