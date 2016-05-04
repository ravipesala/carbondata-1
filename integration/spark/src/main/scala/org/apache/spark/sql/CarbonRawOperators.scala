/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.util.ArrayList

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.LeafNode
import org.apache.spark.sql.hive.CarbonMetastoreCatalog
import org.apache.spark.unsafe.types.UTF8String

import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.carbondata.integration.spark.{RawKeyVal, RawKeyValImpl}
import org.carbondata.integration.spark.cache.QueryPredicateTempCache
import org.carbondata.integration.spark.query.CarbonQueryPlan
import org.carbondata.integration.spark.rdd.CarbonRawQueryRDD
import org.carbondata.integration.spark.util.{CarbonQueryUtil, CarbonScalaUtil}
import org.carbondata.query.carbon.model.{QueryDimension, QueryMeasure}
import org.carbondata.query.expression.{ColumnExpression => CarbonColumnExpression, Expression => CarbonExpression, LiteralExpression => CarbonLiteralExpression}
import org.carbondata.query.expression.arithmetic.{AddExpression, DivideExpression, MultiplyExpression, SubstractExpression}
import org.carbondata.query.expression.conditional._
import org.carbondata.query.expression.logical.{AndExpression, OrExpression}


case class CarbonRawCubeScan(
  var attributes: Seq[Attribute],
  relation: CarbonRelation,
  dimensionPredicates: Seq[Expression])(@transient val oc: SQLContext)
  extends LeafNode {

  val cubeName = relation.cubeName
  val carbonTable = relation.metaData.carbonTable
  val selectedDims = scala.collection.mutable.MutableList[QueryDimension]()
  val selectedMsrs = scala.collection.mutable.MutableList[QueryMeasure]()
  var outputColumns = scala.collection.mutable.MutableList[Attribute]()
  var extraPreds: Seq[Expression] = Nil
  val allDims = new scala.collection.mutable.HashSet[String]()
  // val carbonTable = CarbonMetadata.getInstance().getCarbonTable(cubeName)
  @transient val carbonCatalog = sqlContext.catalog.asInstanceOf[CarbonMetastoreCatalog]


  val buildCarbonPlan: CarbonQueryPlan = {
    val plan: CarbonQueryPlan = new CarbonQueryPlan(relation.schemaName, relation.cubeName)


    var queryOrder: Integer = 0
    attributes.map(
      attr => {
        val carbonDimension = carbonTable.getDimensionByName(carbonTable.getFactTableName
          , attr.name)
        if (carbonDimension != null) {
          // TODO if we can add ordina in carbonDimension, it will be good
          allDims += attr.name
          val dim = new QueryDimension(attr.name)
          dim.setQueryOrder(queryOrder);
          queryOrder = queryOrder + 1
          selectedDims += dim
        } else {
          val carbonMeasure = carbonTable.getMeasureByName(carbonTable.getFactTableName()
            , attr.name)
          if (carbonMeasure != null) {
            val m1 = new QueryMeasure(attr.name)
            m1.setQueryOrder(queryOrder);
            queryOrder = queryOrder + 1
            selectedMsrs += m1
          }
        }
      })
    queryOrder = 0

    selectedDims.foreach(plan.addDimension(_))
    selectedMsrs.foreach(plan.addMeasure(_))

    val orderList = new ArrayList[QueryDimension]()

    plan.setSortedDimemsions(orderList)
    plan.setDetailQuery(true)
    plan.setRawDetailQuery(true)
    plan.setOutLocationPath(
      CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS));
    plan.setQueryId(System.nanoTime() + "");
    if (!dimensionPredicates.isEmpty) {
      val exps = preProcessExpressions(dimensionPredicates)
      val expressionVal = CarbonRawCubeScan.transformExpression(exps.head)
      // adding dimension used in expression in querystats
      expressionVal.getChildren.asScala.filter { x => x.isInstanceOf[CarbonColumnExpression] }
      .map { y => allDims += y.asInstanceOf[CarbonColumnExpression].getColumnName }
      plan.setFilterExpression(expressionVal)
    }
    plan
  }

  def preProcessExpressions(expressions: Seq[Expression]): Seq[Expression] = {
    expressions match {
      case left :: right :: rest => preProcessExpressions(List(And(left, right)) ::: rest)
      case List(left, right) => List(And(left, right))

      case _ => expressions
    }
  }

  def addPushdownFilters(keys: Seq[Expression], filters: Array[Array[Expression]],
    conditions: Option[Expression]) {

    // TODO Values in the IN filter is duplicate. replace the list with set
    val buffer = new ArrayBuffer[Expression]
    keys.zipWithIndex.foreach { a =>
      buffer += In(a._1, filters(a._2)).asInstanceOf[Expression]
    }

    // Let's not pushdown condition. Only filter push down is sufficient.
    // Conditions can be applied on hash join result.
    val cond = if (buffer.size > 1) {
      val e = buffer.remove(0)
      buffer.fold(e)(And(_, _))
    } else {
      buffer.asJava.get(0)
    }

    extraPreds = Seq(cond)
  }

  def inputRdd: CarbonRawQueryRDD[Array[Object], Any] = {
    // Update the FilterExpressions with extra conditions added through join pushdown
    if (!extraPreds.isEmpty) {
      val exps = preProcessExpressions(extraPreds.toSeq)
      val expressionVal = CarbonRawCubeScan.transformExpression(exps.head)
      val oldExpressionVal = buildCarbonPlan.getFilterExpression()
      if (null == oldExpressionVal) {
        buildCarbonPlan.setFilterExpression(expressionVal);
      } else {
        buildCarbonPlan.setFilterExpression(new AndExpression(oldExpressionVal, expressionVal));
      }
    }

    val conf = new Configuration();
    val absoluteTableIdentifier = new AbsoluteTableIdentifier(carbonCatalog.storePath,
      new CarbonTableIdentifier(carbonTable.getDatabaseName, carbonTable.getFactTableName))

    val model = CarbonQueryUtil.createQueryModel(
      absoluteTableIdentifier, buildCarbonPlan, carbonTable)
    model.setForcedDetailRawQuery(true)
    model.setDetailQuery(false)
    val kv: RawKeyVal[Array[Object], Any] = new RawKeyValImpl()
    // setting queryid
    buildCarbonPlan.setQueryId(oc.getConf("queryId", System.nanoTime() + ""))
    // CarbonQueryUtil.updateCarbonExecuterModelWithLoadMetadata(model)
    // CarbonQueryUtil.setPartitionColumn(model, relation.cubeMeta.partitioner.partitionColumn)
    // scalastyle:off println
    println("Selected Table to Query ****** "
            + model.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName())
    // scalastyle:on println

    val cubeCreationTime = carbonCatalog.getCubeCreationTime(relation.schemaName, cubeName)
    val schemaLastUpdatedTime =
      carbonCatalog.getSchemaLastUpdatedTime(relation.schemaName, cubeName)
    val big = new CarbonRawQueryRDD(
      oc.sparkContext,
      model,
      buildCarbonPlan.getFilterExpression,
      kv,
      conf,
      cubeCreationTime,
      schemaLastUpdatedTime,
      carbonCatalog.storePath)
    big
  }

  def doExecute(): RDD[InternalRow] = {
    def toType(obj: Any): Any = {
      obj match {
        case s: String => UTF8String.fromString(s)
        case _ => obj
      }
    }

    inputRdd.map { row =>
      val dims = row._1.map(toType)
      new GenericMutableRow(dims)
    }
  }

  def output: Seq[Attribute] = {
    attributes
  }

}

object CarbonRawCubeScan {
  def transformExpression(expr: Expression): CarbonExpression = {
    expr match {
      case Or(left, right) => new
          OrExpression(transformExpression(left), transformExpression(right))
      case And(left, right) => new
          AndExpression(transformExpression(left), transformExpression(right))
      case EqualTo(left, right) => new
          EqualToExpression(transformExpression(left), transformExpression(right))
      case Not(EqualTo(left, right)) => new
          NotEqualsExpression(transformExpression(left), transformExpression(right))
      case IsNotNull(child) => new
          NotEqualsExpression(transformExpression(child), transformExpression(Literal(null)))
      case Not(In(left, right)) => new NotInExpression(transformExpression(left),
        new ListExpression(right.map(transformExpression).asJava))
      case In(left, right) => new InExpression(transformExpression(left),
        new ListExpression(right.map(transformExpression).asJava))
      case Add(left, right) => new
          AddExpression(transformExpression(left), transformExpression(right))
      case Subtract(left, right) => new
          SubstractExpression(transformExpression(left), transformExpression(right))
      case Multiply(left, right) => new
          MultiplyExpression(transformExpression(left), transformExpression(right))
      case Divide(left, right) => new
          DivideExpression(transformExpression(left), transformExpression(right))
      case GreaterThan(left, right) => new
          GreaterThanExpression(transformExpression(left), transformExpression(right))
      case LessThan(left, right) => new
          LessThanExpression(transformExpression(left), transformExpression(right))
      case GreaterThanOrEqual(left, right) => new
          GreaterThanEqualToExpression(transformExpression(left), transformExpression(right))
      case LessThanOrEqual(left, right) => new
          LessThanEqualToExpression(transformExpression(left), transformExpression(right))
      case AttributeReference(name, dataType, _, _) => new CarbonColumnExpression(name.toString,
        CarbonScalaUtil.convertSparkToCarbonDataType(dataType))
      case Literal(name, dataType) =>
        new CarbonLiteralExpression(QueryPredicateTempCache.instance.getPredicate(name.toString),
          CarbonScalaUtil.convertSparkToCarbonDataType(dataType))
      case Cast(left, right) if (!left.isInstanceOf[Literal]) => transformExpression(left)
      case _ =>
        new SparkUnknownExpression(expr.transform {
          case AttributeReference(name, dataType, _, _) =>
            CarbonBoundReference(new CarbonColumnExpression(name.toString,
              CarbonScalaUtil.convertSparkToCarbonDataType(dataType)), dataType, expr.nullable)
        })
    }
  }
}

