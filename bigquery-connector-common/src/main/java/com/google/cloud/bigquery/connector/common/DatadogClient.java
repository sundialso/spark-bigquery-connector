package com.google.cloud.bigquery.connector.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Client for sending metrics to Datadog API */
public class DatadogClient {
  private static final Logger LOG = LoggerFactory.getLogger(DatadogClient.class);

  private static final String DATADOG_API_URL = "https://api.datadoghq.com/api/v1/series";
  private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static String apiKey;
  private static String appKey;
  private static OkHttpClient httpClient;
  private static boolean initialized = false;
  private static boolean initFailed = false;

  /** Initialize the Datadog client by fetching credentials from AWS Secrets Manager */
  public static synchronized void initialize() {
    if (initialized || initFailed) {
      return;
    }

    LOG.info("Initializing Datadog HTTP API client");
    try {
      // Get API and APP keys from AWS Secrets Manager
      apiKey = SecretManagerUtil.getDatadogApiKey();
      appKey = SecretManagerUtil.getDatadogAppKey();

      if (apiKey == null || appKey == null) {
        LOG.error("Failed to retrieve Datadog API or APP key from AWS Secrets Manager");
        initFailed = true;
        return;
      }

      // Initialize HTTP client
      httpClient =
          new OkHttpClient.Builder()
              .connectTimeout(10, TimeUnit.SECONDS)
              .writeTimeout(10, TimeUnit.SECONDS)
              .readTimeout(30, TimeUnit.SECONDS)
              .build();

      initialized = true;
      LOG.info("Datadog HTTP API client initialized successfully");
    } catch (Exception e) {
      LOG.error("Failed to initialize Datadog HTTP API client", e);
      initFailed = true;
    }
  }

  /**
   * Send BigQuery metrics to Datadog
   *
   * @param tags The tags to send
   * @param metrics The metrics to send
   */
  public static void sendBigQueryMetrics(List<String> tags, Map<String, Object> metrics) {
    if (!initialized && !initFailed) {
      initialize();
    }

    if (!initialized) {
      LOG.warn("Datadog client not initialized, skipping metrics send");
      return;
    }

    try {
      long now = System.currentTimeMillis() / 1000;
      List<ObjectNode> series = new ArrayList<>();

      // Process bytes read
      if (metrics.containsKey("bytes_read")) {
        series.add(
            createMetricPoint(
                "bigquery.metrics.bytes_read", now, toLong(metrics.get("bytes_read")), tags));
      }

      // Process rows read
      if (metrics.containsKey("rows_read")) {
        series.add(
            createMetricPoint(
                "bigquery.metrics.rows_read", now, toLong(metrics.get("rows_read")), tags));
      }

      // Process scan time
      if (metrics.containsKey("scan_time_ms")) {
        series.add(
            createMetricPoint(
                "bigquery.metrics.scan_time_ms", now, toLong(metrics.get("scan_time_ms")), tags));
      }

      // Process parse time
      if (metrics.containsKey("parse_time_ms")) {
        series.add(
            createMetricPoint(
                "bigquery.metrics.parse_time_ms", now, toLong(metrics.get("parse_time_ms")), tags));
      }

      // Process spark time
      if (metrics.containsKey("spark_time_ms")) {
        series.add(
            createMetricPoint(
                "bigquery.metrics.spark_time_ms", now, toLong(metrics.get("spark_time_ms")), tags));
      }

      // Send metrics to Datadog
      if (!series.isEmpty()) {
        sendMetricsToDatadog(series);
      }

      LOG.debug("Sent BigQuery metrics to Datadog for tags: {}", tags);
    } catch (Exception e) {
      LOG.error("Error sending metrics to Datadog", e);
    }
  }

  /** Create a metric point */
  private static ObjectNode createMetricPoint(
      String metricName, long timestamp, long value, List<String> tags) {
    ObjectNode point = MAPPER.createObjectNode();
    point.put("metric", metricName);
    point.put("type", "gauge");

    ArrayNode pointsArray = MAPPER.createArrayNode();
    ArrayNode pointValue = MAPPER.createArrayNode();
    pointValue.add(timestamp);
    pointValue.add(value);
    pointsArray.add(pointValue);
    point.set("points", pointsArray);

    ArrayNode tagsArray = MAPPER.createArrayNode();
    for (String tag : tags) {
      tagsArray.add(tag);
    }
    point.set("tags", tagsArray);

    return point;
  }

  /** Send metrics to Datadog API */
  private static void sendMetricsToDatadog(List<ObjectNode> series) throws IOException {
    ObjectNode root = MAPPER.createObjectNode();
    ArrayNode seriesArray = MAPPER.createArrayNode();

    for (ObjectNode point : series) {
      seriesArray.add(point);
    }

    root.set("series", seriesArray);

    String jsonBody = MAPPER.writeValueAsString(root);
    RequestBody body = RequestBody.create(jsonBody, JSON);

    Request request =
        new Request.Builder()
            .url(DATADOG_API_URL)
            .addHeader("Content-Type", "application/json")
            .addHeader("DD-API-KEY", apiKey)
            .addHeader("DD-APPLICATION-KEY", appKey)
            .post(body)
            .build();

    try (Response response = httpClient.newCall(request).execute()) {
      if (!response.isSuccessful()) {
        LOG.error(
            "Failed to send metrics to Datadog: {} {}",
            response.code(),
            response.body() != null ? response.body().string() : "");
      } else {
        LOG.debug("Successfully sent metrics to Datadog");
      }
    }
  }

  /** Convert an object to a long value */
  private static long toLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    try {
      return Long.parseLong(value.toString());
    } catch (NumberFormatException e) {
      LOG.warn("Could not convert value to long: {}", value);
      return 0;
    }
  }

  /** Shutdown the Datadog client */
  public static void shutdown() {
    if (httpClient != null) {
      LOG.info("Shutting down Datadog HTTP API client");
      httpClient.dispatcher().executorService().shutdown();
      httpClient.connectionPool().evictAll();
      initialized = false;
    }
  }
}
