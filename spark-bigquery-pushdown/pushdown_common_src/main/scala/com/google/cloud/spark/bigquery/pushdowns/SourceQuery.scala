package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory
import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.blockStatement
import org.apache.spark.sql.catalyst.expressions.Attribute

/** The base query representing a BigQuery table
 *
 * @constructor
 * @param tableName   The BigQuery table to be queried
 * @param outputAttributes  Columns used to override the output generation
 *                    These are the columns resolved by DirectBigQueryRelation.
 * @param alias      Query alias.
 */
case class SourceQuery(
    expressionConverter: SparkExpressionConverter,
    expressionFactory: SparkExpressionFactory,
    bigQueryRDDFactory: BigQueryRDDFactory,
    tableName: String,
    outputAttributes: Seq[Attribute],
    alias: String,
    pushdownFilters: Option[String] = None)
  extends BigQuerySQLQuery(
    expressionConverter,
    expressionFactory,
    alias,
    outputAttributes = Some(outputAttributes),
    conjunctionStatement = ConstantString("`" + tableName + "`").toStatement + ConstantString("AS BQ_CONNECTOR_QUERY_ALIAS")) {

    override def find[T](query: PartialFunction[BigQuerySQLQuery, T]): Option[T] = query.lift(this)

    /** Builds the WHERE statement of the source query */
    override val suffixStatement: BigQuerySQLStatement = {
        if(pushdownFilters.isDefined) {
            ConstantString("WHERE ") + pushdownFilters.get
        } else {
            EmptyBigQuerySQLStatement()
        }
    }

    /** Modifies the SELECT clause to read x_col always along with select * */
    override def getStatement(useAlias: Boolean): BigQuerySQLStatement = {
        var selectedColumns = if (columns.isEmpty || columns.get.isEmpty) ConstantString("*").toStatement else columns.get

        if (output.exists(_.name == "_PARTITIONTIME")) {
            selectedColumns = selectedColumns + ConstantString(", _PARTITIONTIME as _PARTITIONTIME")
        }
        if (output.exists(_.name == "_PARTITIONDATE")) {
            selectedColumns = selectedColumns + ConstantString(", _PARTITIONDATE as _PARTITIONDATE")
        }

        val statement =
            ConstantString("SELECT") + selectedColumns + "FROM" +
              sourceStatement + suffixStatement

        if (useAlias) {
            blockStatement(statement, alias)
        } else {
            statement
        }
    }
}
