package com.google.cloud.bigquery.connector.common;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SundialBigQueryMetricsUtil {

  private static final Logger logger = LoggerFactory.getLogger(SundialBigQueryMetricsUtil.class);

  /** Send the read session metrics to Datadog */
  public static synchronized void sendMetricsToDatadog(
      Map<String, String> tags, long rowsRead, long bytesRead, long parseTime, long sparkTime) {
    try {

      // Create tags
      List<String> datadogTags = new ArrayList<>();
      datadogTags.add("application_name:" + tags.get("application_name"));
      datadogTags.add("bigquery_table_id:" + tags.get("bigquery_table_id"));
      datadogTags.add("tenant:" + tags.get("tenant_slug"));
      datadogTags.add("sundial_table_id:" + tags.get("sundial_table_id"));
      datadogTags.add("run_id:" + tags.get("run_id"));

      // Create metrics map
      Map<String, Object> metrics = new HashMap<>();
      metrics.put("bytes_read", bytesRead);
      metrics.put("rows_read", rowsRead);
      metrics.put("scan_time_ms", sparkTime);
      metrics.put("parse_time_ms", parseTime);

      // Send metrics to Datadog
      DatadogClient.sendBigQueryMetrics(datadogTags, metrics);

      logger.debug(
          "Sent BigQuery metrics to Datadog for bigquery_table_id: {}",
          tags.get("bigquery_table_id"));
    } catch (Exception e) {
      logger.error("Failed to send metrics to Datadog", e);
    }
  }
}
