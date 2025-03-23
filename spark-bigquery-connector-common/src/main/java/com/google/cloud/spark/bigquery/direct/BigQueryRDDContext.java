/*
 * Copyright 2022 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.spark.bigquery.direct;

import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.connector.common.BigQueryClientFactory;
import com.google.cloud.bigquery.connector.common.BigQueryStorageReadRowsTracer;
import com.google.cloud.bigquery.connector.common.BigQueryTracerFactory;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.bigquery.connector.common.ReadRowsHelper;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.spark.bigquery.InternalRowIterator;
import com.google.cloud.spark.bigquery.ReadRowsResponseToInternalRowIteratorConverter;
import com.google.cloud.spark.bigquery.SchemaConverters;
import com.google.cloud.spark.bigquery.SchemaConvertersConfiguration;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig;
import com.google.cloud.spark.bigquery.metrics.SparkMetricsSource;
import com.google.common.base.Joiner;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.InterruptibleIterator;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

class BigQueryRDDContext implements Serializable {

  private static long serialVersionUID = -2219993393692435055L;

  private final Partition[] partitions;
  private final ReadSession readSession;
  private final String[] columnsInOrder;
  private final Schema bqSchema;
  private final SparkBigQueryConfig options;
  private final BigQueryClientFactory bigQueryClientFactory;
  private final BigQueryTracerFactory bigQueryTracerFactory;

  private List<String> streamNames;

  public BigQueryRDDContext(
      Partition[] parts,
      ReadSession readSession,
      Schema bqSchema,
      String[] columnsInOrder,
      SparkBigQueryConfig options,
      BigQueryClientFactory bigQueryClientFactory,
      BigQueryTracerFactory bigQueryTracerFactory) {

    this.partitions = parts;
    this.readSession = readSession;
    this.columnsInOrder = columnsInOrder;
    this.bigQueryClientFactory = bigQueryClientFactory;
    this.bigQueryTracerFactory = bigQueryTracerFactory;
    this.options = options;
    this.bqSchema = bqSchema;
    this.streamNames = BigQueryUtil.getStreamNames(readSession);
  }

  public scala.collection.Iterator<InternalRow> compute(Partition split, TaskContext context) {
    BigQueryPartition bigQueryPartition = (BigQueryPartition) split;
    SparkMetricsSource sparkMetricsSource = new SparkMetricsSource();
    SparkEnv.get().metricsSystem().registerSource(sparkMetricsSource);
    SparkConf sparkConf = SparkEnv.get().conf();
    Map<String, String> tracerTags = getTracerTags(sparkConf);

    // Read session metrics are not supported for dsv1
    BigQueryStorageReadRowsTracer tracer =
        bigQueryTracerFactory.newReadRowsTracer(
            tracerTags, Joiner.on(",").join(streamNames), sparkMetricsSource, Optional.empty());

    ReadRowsRequest.Builder request =
        ReadRowsRequest.newBuilder().setReadStream(bigQueryPartition.getStream());

    ReadRowsHelper readRowsHelper =
        new ReadRowsHelper(
            bigQueryClientFactory,
            request,
            options.toReadSessionCreatorConfig().toReadRowsHelperOptions(),
            Optional.of(tracer));
    Iterator<ReadRowsResponse> readRowsResponseIterator = readRowsHelper.readRows();

    StructType schema =
        options
            .getSchema()
            .orElse(
                SchemaConverters.from(SchemaConvertersConfiguration.from(options))
                    .toSpark(bqSchema));

    ReadRowsResponseToInternalRowIteratorConverter converter;
    if (options.getReadDataFormat().equals(DataFormat.AVRO)) {
      converter =
          ReadRowsResponseToInternalRowIteratorConverter.avro(
              bqSchema,
              Arrays.asList(columnsInOrder),
              readSession.getAvroSchema().getSchema(),
              Optional.of(schema),
              Optional.of(tracer),
              SchemaConvertersConfiguration.from(options),
              options.getResponseCompressionCodec());
    } else {
      converter =
          ReadRowsResponseToInternalRowIteratorConverter.arrow(
              Arrays.asList(columnsInOrder),
              readSession.getArrowSchema().getSerializedSchema(),
              Optional.of(schema),
              Optional.of(tracer),
              options.getResponseCompressionCodec());
    }

    return new InterruptibleIterator<InternalRow>(
        context,
        new ScalaIterator<InternalRow>(
            new InternalRowIterator(readRowsResponseIterator, converter, readRowsHelper, tracer)));
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
    tracerTags.put("bigquery_table_id", "query_pushdown");
    return tracerTags;
  }
  // CPD-ON

  public Partition[] getPartitions() {
    return partitions;
  }
}
