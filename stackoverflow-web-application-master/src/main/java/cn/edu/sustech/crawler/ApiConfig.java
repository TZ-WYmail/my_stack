package cn.edu.sustech.crawler;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ApiConfig {
    private static final Properties properties = new Properties();

    static {
        try (InputStream input = ApiConfig.class.getClassLoader().getResourceAsStream("config.properties")) {
            // 加载配置文件
            properties.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
            // 可以根据需要做额外的处理，比如设置默认值或抛出异常
        }
    }
    public static final String BASE_URL = properties.getProperty("base_url");
    public static final String API_KEY = properties.getProperty("api_key");
    public static final String SITE = properties.getProperty("site");
    public static final String USER_AGENT = properties.getProperty("user_agent");
    public static final int MAX_RETRY_ATTEMPTS = Integer.parseInt(properties.getProperty("max_retry_attempts", "10"));
    public static final int RETRY_DELAY_MS = Integer.parseInt(properties.getProperty("retry_delay_ms", "1000"));
}
