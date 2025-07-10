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

package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.blockStatement
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, CheckOverflow, Expression, Like, ScalarSubquery, TimestampAdd, UnaryMinus}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * Convert Spark 3.3 specific expressions to SQL
 */
class Spark33ExpressionConverter(expressionFactory: SparkExpressionFactory, sparkPlanFactory: SparkPlanFactory) extends SparkExpressionConverter() {

  // Spark 3.4+ has TimestampAdd, so we add specific handling for it in here rather than in the common folder so that
  // build for other Spark versions don't fail.
  // Though this folder says Spark 3.3 - we've made it compatible with Spark 3.5 in our fork - so for all intents and
  // purposes, this is will be built wth Spark 3.5.
  override def convertDateExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case TimestampAdd(unit, quantity, startDate, _) =>
        ConstantString("DATETIME_ADD") +
          blockStatement(
            convertStatement(startDate, fields) + ", INTERVAL " +
            convertStatement(quantity, fields) + " " + unit
          )
      case _ => 
        // Call parent method for other date expressions
        super.convertDateExpressions(expression, fields).orNull
    })
  }

  override def convertScalarSubqueryExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
    expression match {
      case ScalarSubquery(plan, _, _, joinCond, _, _) if joinCond.isEmpty =>
        blockStatement(new Spark33BigQueryStrategy(this, expressionFactory, sparkPlanFactory)
          .generateQueryFromPlan(plan).get.getStatement())
    }
  }

  override def convertCheckOverflowExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
    expression match {
      case CheckOverflow(child, t, _) =>
        getCastType(t) match {
          case Some(cast) =>
            ConstantString("CAST") +
              blockStatement(convertStatement(child, fields) + "AS" + cast)
          case _ => convertStatement(child, fields)
        }
    }
  }

  override def convertUnaryMinusExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
    expression match {
      case UnaryMinus(child, _) =>
        ConstantString("-") +
          blockStatement(convertStatement(child, fields))
    }
  }

  override def convertCastExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
    expression match {
      case Cast(child, dataType, _, evalMode) if evalMode.toString != "ANSI" =>
        performCastExpressionConversion(child, fields, dataType)
    }
  }

  override def convertLikeExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
    expression match {
      case Like(left, right, _) =>
        convertStatement(left, fields) + "LIKE" + convertStatement(right, fields)
    }
  }
}
