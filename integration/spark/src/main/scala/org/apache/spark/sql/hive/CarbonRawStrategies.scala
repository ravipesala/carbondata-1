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


package org.apache.spark.sql.hive

import scala.collection.JavaConverters._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, QueryPlanner}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{Filter, Project, SparkPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation

import org.carbondata.common.logging.LogServiceFactory

class CarbonRawStrategies(sqlContext: SQLContext) extends QueryPlanner[SparkPlan] {

  override def strategies: Seq[Strategy] = getStrategies

  val LOGGER = LogServiceFactory.getLogService("CarbonRawStrategies")

  def getStrategies: Seq[Strategy] = {
    val total = sqlContext.planner.strategies :+ CarbonRawCubeScans
    total
  }

  /**
   * Carbon strategies for Carbon cube scanning
   */
  private[sql] object CarbonRawCubeScans extends Strategy {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case PhysicalOperation(projectList, predicates,
        l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)) =>
          carbonRawScan(projectList,
            predicates,
            carbonRelation,
            None,
            None,
            None,
            false,
            true,
            false)(sqlContext)._1 :: Nil

        case catalyst.planning.PartialAggregation(
        namedGroupingAttributes,
        rewrittenAggregateExpressions,
        groupingExpressions,
        partialComputation,
        PhysicalOperation(projectList, predicates,
        l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _))) =>
          handleRawAggregation(plan, plan, projectList, predicates, carbonRelation,
            partialComputation, groupingExpressions, namedGroupingAttributes,
            rewrittenAggregateExpressions)
        case CarbonDictionaryCatalystDecoder(relations, profile, aliasMap, child) =>
          CarbonDictionaryDecoder(relations, profile, aliasMap, planLater(child))(sqlContext) :: Nil
        case _ =>
          Nil
      }
    }


    def handleRawAggregation(plan: LogicalPlan,
      aggPlan: LogicalPlan,
      projectList: Seq[NamedExpression],
      predicates: Seq[Expression],
      carbonRelation: CarbonDatasourceRelation,
      partialComputation: Seq[NamedExpression],
      groupingExpressions: Seq[Expression],
      namedGroupingAttributes: Seq[Attribute],
      rewrittenAggregateExpressions: Seq[NamedExpression]):
    Seq[SparkPlan] = {
      val (_, _, _, aliases, groupExprs, substitutesortExprs, limitExpr) = extractPlan(plan)

      val s =
        try { {
          carbonRawScan(projectList,
            predicates,
            carbonRelation,
            Some(partialComputation),
            substitutesortExprs,
            limitExpr,
            !groupingExpressions.isEmpty,
            false,
            true)(sqlContext)
        }
        } catch {
          case _ => null
        }

      if (s != null) {
        if (!s._2) {
          CarbonAggregate(
            partial = false,
            namedGroupingAttributes,
            rewrittenAggregateExpressions,
            CarbonRawAggregate(
              partial = true,
              groupingExpressions,
              partialComputation,
              s._1))(sqlContext) :: Nil
        } else {
          Nil
        }
      } else {
        Nil
      }
    }

    /**
     * Create carbon scan
     */
    private def carbonRawScan(projectList: Seq[NamedExpression],
      predicates: Seq[Expression],
      relation: CarbonDatasourceRelation,
      groupExprs: Option[Seq[Expression]],
      substitutesortExprs: Option[Seq[SortOrder]],
      limitExpr: Option[Expression],
      isGroupByPresent: Boolean,
      detailQuery: Boolean,
      useBinaryAggregation: Boolean)(sc: SQLContext): (SparkPlan, Boolean) = {

      val tableName: String =
        relation.carbonRelation.metaData.carbonTable.getFactTableName.toLowerCase
      if (detailQuery == false) {
        val projectSet = AttributeSet(projectList.flatMap(_.references))
        val s = CarbonRawCubeScan(
          projectSet.toSeq,
          relation.carbonRelation,
          predicates,
          groupExprs,
          substitutesortExprs,
          limitExpr,
          isGroupByPresent,
          detailQuery,
          useBinaryAggregation)(sqlContext)
        if (s.attributesNeedToDecode.size() > 0) {
          val relations = Seq((tableName, relation)).toMap
          val attrs = s.attributesNeedToDecode.asScala.toSeq.map { attr =>
            AttributeReference(attr.name,
              attr.dataType,
              attr.nullable,
              attr.metadata)(attr.exprId, Seq(tableName))
          }
          val predExpr = predicates.reduceLeft(And)
          (Project(projectList, Filter(predicates.reduceLeft(And),
            CarbonDictionaryDecoder(relations, IncludeProfile(attrs), Map(), s)(sc))),
            true)
        } else {
          (s, s.buildCarbonPlan.getDimAggregatorInfos.size() > 0)
        }

      }
      else {
        val projectSet = AttributeSet(projectList.flatMap(_.references))
        val s = CarbonRawCubeScan(projectSet.toSeq,
          relation.carbonRelation,
          predicates,
          groupExprs,
          substitutesortExprs,
          limitExpr,
          isGroupByPresent,
          detailQuery,
          useBinaryAggregation)(sqlContext)
        if (s.attributesNeedToDecode.size() > 0) {
          val relations = Seq((tableName, relation)).toMap
          val attrs = s.attributesNeedToDecode.asScala.toSeq.map { attr =>
            AttributeReference(attr.name,
              attr.dataType,
              attr.nullable,
              attr.metadata)(attr.exprId, Seq(tableName))
          }
          (Project(projectList, Filter(predicates.reduceLeft(And),
            CarbonDictionaryDecoder(relations, IncludeProfile(attrs), Map(), s)(sc))),
            true)
        } else {
          (Project(projectList, s), s.buildCarbonPlan.getDimAggregatorInfos.size() > 0)
        }
      }
    }

    private def extractPlan(plan: LogicalPlan) = {
      val (a, b, c, aliases, groupExprs, sortExprs, limitExpr) =
        PhysicalOperation1.collectProjectsAndFilters(plan)
      val substitutesortExprs = sortExprs match {
        case Some(sort) =>
          Some(sort.map {
            case SortOrder(a: Alias, direction) =>
              val ref = aliases.getOrElse(a.toAttribute, a) match {
                case Alias(ref, name) => ref
                case others => others
              }
              SortOrder(ref, direction)
            case others => others
          })
        case others => others
      }
      (a, b, c, aliases, groupExprs, substitutesortExprs, limitExpr)
    }
  }

}
