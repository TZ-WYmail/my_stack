package cn.edu.sustech.crawler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ApiClient {
    private static final Logger logger = LoggerFactory.getLogger(ApiClient.class);
    private final OkHttpClient client;

    public ApiClient() {
        this.client = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(30, TimeUnit.SECONDS)
                .build();
    }

    public JSONObject executeRequest(String endpoint, String params) {
        String url = String.format("%s/%s?%s&site=%s&key=%s",
                ApiConfig.BASE_URL, endpoint, params, ApiConfig.SITE, ApiConfig.API_KEY);

        Request request = new Request.Builder()
                .url(url)
                .header("User-Agent", ApiConfig.USER_AGENT)
                .build();

        for (int attempt = 1; attempt <= ApiConfig.MAX_RETRY_ATTEMPTS; attempt++) {
            Response response = null;
            try {
                response = client.newCall(request).execute();
                if (!response.isSuccessful()) {
                    throw new ApiException("API request failed with code: " + response.code());
                }

                assert response.body() != null;
                String responseBody = response.body().string();
                JSONObject data = JSON.parseObject(responseBody);

                if (data == null) {
                    throw new ApiException("Failed to parse JSON response");
                }

                // Check for API-specific error responses
                if (data.containsKey("error")) {
                    throw new ApiException("API error: " + data.getJSONObject("error").getString("message"));
                }
                return data;

            } catch (IOException | ApiException e) {
                if (attempt == ApiConfig.MAX_RETRY_ATTEMPTS) {
                    throw new ApiException("Failed after " + attempt + " attempts", e);
                }
                logger.warn("Request failed (attempt {}/{}), retrying...", attempt, ApiConfig.MAX_RETRY_ATTEMPTS);
                try {
                    // 指数退避算法，重试延迟时间逐渐增加，最大 64 秒
                    Thread.sleep((long) ApiConfig.RETRY_DELAY_MS * (int) Math.min(64, Math.pow(2, attempt)));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new ApiException("Request interrupted", ie);
                }

                logger.info("Retrying request to {}", url);
                logger.info("request: {}", request);
                logger.info("response: {}", response);
            } finally {
                if (response != null) {
                    response.close();
                }
            }
        }
        throw new ApiException("Failed to execute request after all retries");
    }
}
