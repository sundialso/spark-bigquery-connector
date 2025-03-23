/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.v2.context;

import static com.google.common.base.Optional.fromJavaUtil;

import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.connector.common.ReadSessionResponse;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions.ResponseCompressionCodec;
import com.google.cloud.spark.bigquery.metrics.SparkBigQueryReadSessionMetrics;
import com.google.cloud.spark.bigquery.metrics.SparkMetricsSource;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.vectorized.ColumnarBatch;

public class ArrowInputPartitionContext implements InputPartitionContext<ColumnarBatch> {

  private final String tableNameOrQuery;
  private final BigQueryClientFactory bigQueryReadClientFactory;
  private final BigQueryTracerFactory tracerFactory;
  private List<String> streamNames;
  private final ReadRowsHelper.Options options;
  private final ImmutableList<String> selectedFields;
  private final ByteString serializedArrowSchema;
  private final com.google.common.base.Optional<StructType> userProvidedSchema;
  private final SparkBigQueryReadSessionMetrics sparkBigQueryReadSessionMetrics;
  private final ResponseCompressionCodec responseCompressionCodec;

  public ArrowInputPartitionContext(
      String tableNameOrQuery,
      BigQueryClientFactory bigQueryReadClientFactory,
      BigQueryTracerFactory tracerFactory,
      List<String> names,
      ReadRowsHelper.Options options,
      ImmutableList<String> selectedFields,
      ReadSessionResponse readSessionResponse,
      Optional<StructType> userProvidedSchema,
      SparkBigQueryReadSessionMetrics sparkBigQueryReadSessionMetrics,
      ResponseCompressionCodec responseCompressionCodec) {
    this.tableNameOrQuery = tableNameOrQuery;
    this.bigQueryReadClientFactory = bigQueryReadClientFactory;
    this.streamNames = names;
    this.options = options;
    this.selectedFields = selectedFields;
    this.serializedArrowSchema =
        readSessionResponse.getReadSession().getArrowSchema().getSerializedSchema();
    this.tracerFactory = tracerFactory;
    this.userProvidedSchema = fromJavaUtil(userProvidedSchema);
    this.sparkBigQueryReadSessionMetrics = sparkBigQueryReadSessionMetrics;
    this.responseCompressionCodec = responseCompressionCodec;
  }

  public InputPartitionReaderContext<ColumnarBatch> createPartitionReaderContext() {
    SparkMetricsSource sparkMetricsSource = new SparkMetricsSource();

    TaskContext.get()
        .registerAccumulator(sparkBigQueryReadSessionMetrics.getBytesReadAccumulator());
    TaskContext.get().registerAccumulator(sparkBigQueryReadSessionMetrics.getRowsReadAccumulator());
    TaskContext.get()
        .registerAccumulator(sparkBigQueryReadSessionMetrics.getParseTimeAccumulator());
    TaskContext.get().registerAccumulator(sparkBigQueryReadSessionMetrics.getScanTimeAccumulator());

    SparkEnv.get().metricsSystem().registerSource(sparkMetricsSource);
    SparkConf sparkConf = SparkEnv.get().conf();
    Map<String, String> tracerTags = getTracerTags(sparkConf);
    BigQueryStorageReadRowsTracer tracer =
        tracerFactory.newReadRowsTracer(
            tracerTags,
            Joiner.on(",").join(streamNames),
            sparkMetricsSource,
            Optional.of(sparkBigQueryReadSessionMetrics));
    List<ReadRowsRequest.Builder> readRowsRequests =
        streamNames.stream()
            .map(name -> ReadRowsRequest.newBuilder().setReadStream(name))
            .collect(Collectors.toList());
    ReadRowsHelper readRowsHelper =
        new ReadRowsHelper(bigQueryReadClientFactory, readRowsRequests, options);
    tracer.startStream();
    Iterator<ReadRowsResponse> readRowsResponses = readRowsHelper.readRows();

    return new ArrowColumnBatchPartitionReaderContext(
        readRowsResponses,
        serializedArrowSchema,
        readRowsHelper,
        selectedFields,
        tracer,
        userProvidedSchema.toJavaUtil(),
        options.numBackgroundThreads(),
        responseCompressionCodec);
  }

  // CPD-OFF
  private Map<String, String> getTracerTags(SparkConf sparkConf) {
    Map<String, String> tracerTags = new HashMap<>();
    tracerTags.put("application_name", sparkConf.get("spark.app.name", "unknown_application_name"));
    tracerTags.put(
        "tenant_slug", sparkConf.get("spark.sundial.tenant_slug", "unknown_tenant_slug"));
    tracerTags.put(
        "sundial_table_id", sparkConf.get("spark.sundial.table_id", "unknown_sundial_table_id"));
    tracerTags.put("run_id", sparkConf.get("spark.sundial.run_id", "unknown_run_id"));
    tracerTags.put("bigquery_table_id", tableNameOrQuery);
    return tracerTags;
  }
  // CPD-ON

  @Override
  public boolean supportColumnarReads() {
    return true;
  }
}
