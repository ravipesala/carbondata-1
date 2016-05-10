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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.UTF8String

import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.integration.spark.{RawKeyVal, RawKeyValImpl}
import org.carbondata.integration.spark.query.CarbonQueryPlan
import org.carbondata.integration.spark.rdd.CarbonRawQueryRDD
import org.carbondata.integration.spark.util.{CarbonQueryUtil, CarbonScalaUtil}
import org.carbondata.query.carbon.model.{QueryDimension, QueryMeasure, QuerySchemaInfo}
import org.carbondata.query.carbon.result.BatchRawResult
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper
import org.carbondata.query.expression.{ColumnExpression => CarbonColumnExpression, Expression => CarbonExpression, LiteralExpression => CarbonLiteralExpression}
import org.carbondata.query.expression.conditional._
import org.carbondata.query.expression.logical.{AndExpression, OrExpression}


case class CarbonRawCubeScan(var attributesRaw: Seq[Attribute],
  relationRaw: CarbonRelation,
  dimensionPredicatesRaw: Seq[Expression],
  aggExprsRaw: Option[Seq[Expression]],
  sortExprsRaw: Option[Seq[SortOrder]],
  limitExprRaw: Option[Expression],
  isGroupByPresentRaw: Boolean,
  detailQueryRaw: Boolean = false,
  useBinaryAggregator: Boolean)(@transient val ocRaw: SQLContext)
  extends AbstractCubeScan(attributesRaw,
    relationRaw,
    dimensionPredicatesRaw,
    aggExprsRaw,
    sortExprsRaw,
    limitExprRaw,
    isGroupByPresentRaw,
    detailQueryRaw)(ocRaw) {

  override def processAggregateExpr(plan: CarbonQueryPlan, currentAggregate: AggregateExpression1,
    queryOrder: Int): Int = {

    currentAggregate match {
      case Sum(attr: AttributeReference) =>
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.length > 0) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.SUM)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims.length > 0) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "sum", d1.getQueryOrder)
          }
        }
        queryOrder + 1

      case Count(attr: AttributeReference) =>
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.length > 0) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.COUNT)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims.length > 0) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "count", d1.getQueryOrder)
          }
        }
        queryOrder + 1
      case Count(Literal(star, _)) =>
        val m1 = new QueryMeasure("count(*)")
        m1.setAggregateFunction(CarbonCommonConstants.COUNT)
        m1.setQueryOrder(queryOrder)
        plan.addMeasure(m1)
        plan.setCountStartQuery(true)
        queryOrder + 1
      case CountDistinct(attr: AttributeReference) =>
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.length > 0) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.DISTINCT_COUNT)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims.length > 0) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "distinct-count", d1.getQueryOrder)
          }
        }
        queryOrder + 1

      case Average(attr: AttributeReference) =>
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.length > 0) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.AVERAGE)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims.length > 0) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "avg", d1.getQueryOrder)
          }
        }
        queryOrder + 1

      case Min(attr: AttributeReference) =>
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.length > 0) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.MIN)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims != null) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "min", d1.getQueryOrder)
          }
        }
        queryOrder + 1

      case Max(attr: AttributeReference) =>
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.length > 0) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.MAX)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims.length > 0) {
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "max", d1.getQueryOrder)
          }
        }
        queryOrder + 1

      case SumDistinct(attr: AttributeReference) =>
        val msrs = selectedMsrs.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
        if (msrs.length > 0) {
          val m1 = new QueryMeasure(attr.name)
          m1.setAggregateFunction(CarbonCommonConstants.SUM_DISTINCT)
          m1.setQueryOrder(queryOrder)
          plan.addMeasure(m1)
        } else {
          val dims = selectedDims.filter(m => m.getColumnName.equalsIgnoreCase(attr.name))
          if (dims != null) {
            //            plan.removeDimensionFromDimList(dims(0));
            val d1 = new QueryDimension(attr.name)
            d1.setQueryOrder(queryOrder)
            plan.addAggDimAggInfo(d1.getColumnName, "sum-distinct", queryOrder)
          }
        }
        queryOrder + 1
      case _ => throw new
          Exception("Some Aggregate functions cannot be pushed, force to detailequery")
    }
  }

  override def processFilterExpressions(plan: CarbonQueryPlan) {
    if (!dimensionPredicatesRaw.isEmpty) {
      val expressionVal = processExpression(dimensionPredicatesRaw)
      expressionVal match {
        case Some(ce) =>
          // adding dimension used in expression in querystats
          ce.getChildren.asScala.filter { x => x.isInstanceOf[CarbonColumnExpression] }
          .map { y => allDims += y.asInstanceOf[CarbonColumnExpression].getColumnName }
          plan.setFilterExpression(ce)
        case _ =>
      }
    }
    processExtraAttributes(plan)
  }

  private def processExtraAttributes(plan: CarbonQueryPlan) {
    if (attributesNeedToDecode.size() > 0) {
      val attributeOut = new ArrayBuffer[Attribute]() ++ attributesRaw

      attributesNeedToDecode.asScala.map { attr =>
        val dims = plan.getDimensions.asScala.filter(f => f.getColumnName.equals(attr.name))
        val msrs = plan.getMeasures.asScala.filter(f => f.getColumnName.equals(attr.name))
        var order = plan.getDimensions.size() + plan.getMeasures.size()
        if (dims.size == 0 && msrs.size == 0) {
          val dimension = carbonTable.getDimensionByName(carbonTable.getFactTableName, attr.name)
          if (dimension != null) {
            val qDim = new QueryDimension(dimension.getColName)
            qDim.setQueryOrder(order)
            plan.addDimension(qDim)
            attributeOut += attr
            order += 1
          } else {
            val measure = carbonTable.getMeasureByName(carbonTable.getFactTableName, attr.name)
            if (measure != null) {
              val qMsr = new QueryMeasure(measure.getColName)
              qMsr.setQueryOrder(order)
              plan.addMeasure(qMsr)
              order += 1
              attributeOut += attr
            }
          }
        }
      }
      attributesRaw = attributeOut
    }
  }

  def processExpression(exprs: Seq[Expression]): Option[CarbonExpression] = {
    def transformExpression(expr: Expression): Option[CarbonExpression] = {
      expr match {
        case Or(left, right) =>
          for {
            leftFilter <- transformExpression(left)
            rightFilter <- transformExpression(right)
          } yield {
            new OrExpression(leftFilter, rightFilter)
          }

        case And(left, right) =>
          (transformExpression(left) ++ transformExpression(right)).reduceOption(new
              AndExpression(_, _))

        case EqualTo(a: Attribute, FakeCarbonCast(l@Literal(v, t), b)) => new
            Some(new EqualToExpression(transformExpression(a).get, transformExpression(l).get))
        case EqualTo(FakeCarbonCast(l@Literal(v, t), b), a: Attribute) => new
            Some(new EqualToExpression(transformExpression(a).get, transformExpression(l).get))
        case EqualTo(Cast(a: Attribute, _), FakeCarbonCast(l@Literal(v, t), b)) => new
            Some(new EqualToExpression(transformExpression(a).get, transformExpression(l).get))
        case EqualTo(FakeCarbonCast(l@Literal(v, t), b), Cast(a: Attribute, _)) => new
            Some(new EqualToExpression(transformExpression(a).get, transformExpression(l).get))

        case Not(EqualTo(a: Attribute, FakeCarbonCast(l@Literal(v, t), b))) => new
            Some(new NotEqualsExpression(transformExpression(a).get, transformExpression(l).get))
        case Not(EqualTo(FakeCarbonCast(l@Literal(v, t), b), a: Attribute)) => new
            Some(new NotEqualsExpression(transformExpression(a).get, transformExpression(l).get))
        case Not(EqualTo(Cast(a: Attribute, _), FakeCarbonCast(l@Literal(v, t), b))) => new
            Some(new NotEqualsExpression(transformExpression(a).get, transformExpression(l).get))
        case Not(EqualTo(FakeCarbonCast(l@Literal(v, t), b), Cast(a: Attribute, _))) => new
            Some(new NotEqualsExpression(transformExpression(a).get, transformExpression(l).get))

        case Not(In(a: Attribute, list)) if !list.exists(!_.isInstanceOf[FakeCarbonCast]) =>
          Some(new NotInExpression(transformExpression(a).get,
            new ListExpression(list.map(transformExpression(_).get).asJava)))
        case In(a: Attribute, list) if !list.exists(!_.isInstanceOf[FakeCarbonCast]) =>
          Some(new InExpression(transformExpression(a).get,
            new ListExpression(list.map(transformExpression(_).get).asJava)))
        case Not(In(Cast(a: Attribute, _), list))
          if !list.exists(!_.isInstanceOf[FakeCarbonCast]) =>
          Some(new NotInExpression(transformExpression(a).get,
            new ListExpression(list.map(transformExpression(_).get).asJava)))
        case In(Cast(a: Attribute, _), list) if !list.exists(!_.isInstanceOf[FakeCarbonCast]) =>
          Some(new InExpression(transformExpression(a).get,
            new ListExpression(list.map(transformExpression(_).get).asJava)))

        case AttributeReference(name, dataType, _, _) =>
          Some(new CarbonColumnExpression(name.toString,
            CarbonScalaUtil.convertSparkToCarbonDataType(dataType)))
        case FakeCarbonCast(literal, dataType) => transformExpression(literal)
        case Literal(name, dataType) => Some(new
            CarbonLiteralExpression(name, CarbonScalaUtil.convertSparkToCarbonDataType(dataType)))
        case Cast(left, right) if (!left.isInstanceOf[Literal]) => transformExpression(left)
        case others =>
          others.collect {
            case attr: AttributeReference => attributesNeedToDecode.add(attr)
          }
          unprocessedExprs += others
          None
      }
    }
    exprs.flatMap(transformExpression).reduceOption(new AndExpression(_, _))
  }

  def inputRdd: CarbonRawQueryRDD[BatchRawResult, Any] = {

    val conf = new Configuration();
    val absoluteTableIdentifier =
      new AbsoluteTableIdentifier(carbonCatalog.storePath,
        new CarbonTableIdentifier(carbonTable.getDatabaseName,
          carbonTable.getFactTableName))

    val model = CarbonQueryUtil.createQueryModel(
      absoluteTableIdentifier, buildCarbonPlan, carbonTable)
    model.setForcedDetailRawQuery(true)
    model.setDetailQuery(false)
    val kv: RawKeyVal[BatchRawResult, Any] = new RawKeyValImpl()
    // setting queryid
    buildCarbonPlan.setQueryId(oc.getConf("queryId", System.nanoTime() + ""))
    // scalastyle:off println
    println("Selected Table to Query ****** "
      + model.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableName())
    // scalastyle:on println

    val cubeCreationTime = carbonCatalog
                           .getCubeCreationTime(relationRaw.schemaName, cubeName)
    val schemaLastUpdatedTime = carbonCatalog
                                .getSchemaLastUpdatedTime(relationRaw.schemaName, cubeName)
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

  override def doExecute(): RDD[InternalRow] = {
    def toType(obj: Any): Any = {
      obj match {
        case s: String => UTF8String.fromString(s)
        case _ => obj
      }
    }

    if (useBinaryAggregator) {
      inputRdd.map { row =>
        //      val dims = row._1.map(toType)
        new CarbonRawMutableRow(row._1.getAllRows, row._1.getQuerySchemaInfo)
      }
    } else {
      inputRdd.flatMap { row =>
        val buffer = new ArrayBuffer[GenericMutableRow]()
        while (row._1.hasNext) {
          buffer += new GenericMutableRow(row._1.next().asInstanceOf[Array[Any]])
        }
        buffer
      }
    }
  }

  override def output: Seq[Attribute] = {
    attributesRaw
  }
}

class CarbonRawMutableRow(values: Array[Array[Object]],
  val schema: QuerySchemaInfo) extends GenericMutableRow(values.asInstanceOf[Array[Any]]) {

  val dimsLen = schema.getQueryDimensions.length - 1;
  val order = schema.getQueryOrder
  var counter = 0;

  def getKey(): ByteArrayWrapper = values(0)(counter).asInstanceOf[ByteArrayWrapper]

  def parseKey(key: ByteArrayWrapper, aggData: Array[Object]): Array[Object] = {
    BatchRawResult.parseData(key, aggData, schema);
  }

  def hasNext(): Boolean = {
    counter < values(0).length
  }

  def next(): Unit = {
    counter += 1
  }

  override def numFields: Int = dimsLen + schema.getQueryMeasures.length

  override def anyNull: Boolean = true

  override def get(ordinal: Int, dataType: DataType): AnyRef = {
    values(order(ordinal) - dimsLen)(counter)
    .asInstanceOf[AnyRef]
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    UTF8String
    .fromString(values(
      order(ordinal) - dimsLen)(counter)
                .asInstanceOf[String])
  }

  override def getDouble(ordinal: Int): Double = {
    values(order(ordinal) - dimsLen)(counter)
    .asInstanceOf[Double]
  }

  override def getFloat(ordinal: Int): Float = {
    values(order(ordinal) - dimsLen)(counter)
    .asInstanceOf[Float]
  }

  override def getLong(ordinal: Int): Long = {
    values(order(ordinal) - dimsLen)(counter)
    .asInstanceOf[Long]
  }

  override def getByte(ordinal: Int): Byte = {
    values(order(ordinal) - dimsLen)(counter)
    .asInstanceOf[Byte]
  }

  override def getDecimal(ordinal: Int,
    precision: Int,
    scale: Int): Decimal = {
    values(order(ordinal) - dimsLen)(counter).asInstanceOf[Decimal]
  }

  override def getBoolean(ordinal: Int): Boolean = {
    values(order(ordinal) - dimsLen)(counter)
    .asInstanceOf[Boolean]
  }

  override def getShort(ordinal: Int): Short = {
    values(order(ordinal) - dimsLen)(counter)
    .asInstanceOf[Short]
  }

  override def getInt(ordinal: Int): Int = {
    values(order(ordinal) - dimsLen)(counter)
    .asInstanceOf[Int]
  }

  override def isNullAt(ordinal: Int): Boolean = values(order(ordinal) - dimsLen)(counter) == null
}
