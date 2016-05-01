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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, QueryPlanner}
import org.apache.spark.sql.catalyst.plans.logical.{BroadcastHint, LogicalPlan}
import org.apache.spark.sql.cubemodel._
import org.apache.spark.sql.execution.{Aggregate, DescribeCommand => RunnableDescribeCommand, ExecutedCommand, Project, SparkPlan}
import org.apache.spark.sql.execution.datasources.{DescribeCommand => LogicalDescribeCommand}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.execution.DescribeHiveTableCommand

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.integration.spark.util.CarbonSparkInterFaceLogEvent

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
          carbonScan(projectList, predicates, carbonRelation.carbonRelation) :: Nil

        case catalyst.planning.PartialAggregation(
        namedGroupingAttributes,
        rewrittenAggregateExpressions,
        groupingExpressions,
        partialComputation,
        PhysicalOperation(projectList, predicates,
        l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _))) =>
          val s = carbonScan(projectList, predicates, carbonRelation.carbonRelation, false)
          Aggregate(
              partial = false,
              namedGroupingAttributes,
              rewrittenAggregateExpressions,
              Aggregate(
                partial = true,
                groupingExpressions,
                partialComputation,
                s)) :: Nil

        case ShowCubeCommand(schemaName) =>
          ExecutedCommand(ShowAllCubesInSchema(schemaName, plan.output)) :: Nil
        case c@ShowAllCubeCommand() =>
          ExecutedCommand(ShowAllCubes(plan.output)) :: Nil
        case ShowCreateCubeCommand(cm) =>
          ExecutedCommand(ShowCreateCube(cm, plan.output)) :: Nil
        case ShowTablesDetailedCommand(schemaName) =>
          ExecutedCommand(ShowAllTablesDetail(schemaName, plan.output)) :: Nil
        case ShowAggregateTablesCommand(schemaName) =>
          ExecutedCommand(ShowAggregateTables(schemaName, plan.output)) :: Nil
        case ShowLoadsCommand(schemaName, cube, limit) =>
          ExecutedCommand(ShowLoads(schemaName, cube, limit, plan.output)) :: Nil
        case DescribeFormattedCommand(sql, tblIdentifier) =>
          val isCube = CarbonEnv.getInstance(sqlContext).carbonCatalog
                       .cubeExists(tblIdentifier)(sqlContext);
          if (isCube) {
            val describe = LogicalDescribeCommand(UnresolvedRelation(tblIdentifier, None), false)
            val resolvedTable = sqlContext.executePlan(describe.table).analyzed
            val resultPlan = sqlContext.executePlan(resolvedTable).executedPlan
            ExecutedCommand(DescribeCommandFormatted(resultPlan, plan.output, tblIdentifier)) :: Nil
          }
          else {
            ExecutedCommand(DescribeNativeCommand(sql, plan.output)) :: Nil
          }
        case describe@LogicalDescribeCommand(table, isExtended) =>
          val resolvedTable = sqlContext.executePlan(describe.table).analyzed
          resolvedTable match {
            case t: MetastoreRelation =>
              ExecutedCommand(DescribeHiveTableCommand(t, describe.output, describe.isExtended)) ::
              Nil
            case o: LogicalPlan =>
              val resultPlan = sqlContext.executePlan(o).executedPlan
              ExecutedCommand(
                RunnableDescribeCommand(resultPlan, describe.output, describe.isExtended)) :: Nil
          }
        case _ =>
          Nil
      }
    }

    private def allAggregates(exprs: Seq[Expression]) = {
      exprs.flatMap(_.collect { case a: AggregateExpression => a })
    }

    private def canPushDownJoin(otherRDDPlan: LogicalPlan,
                                joinCondition: Option[Expression]): Boolean = {
      val pushdowmJoinEnabled = sqlContext.sparkContext.conf
                                .getBoolean("spark.carbon.pushdown.join.as.filter", true)

      if (!pushdowmJoinEnabled) {
        return false
      }

      val isJoinOnCarbonCube = otherRDDPlan match {
        case other@PhysicalOperation(projectList, predicates,
        l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)) => true
        case _ => false
      }

      // TODO remove the isJoinOnCarbonCube check
      if (isJoinOnCarbonCube) {
        return false // For now If both left & right are carbon cubes, let's not join
      }

      otherRDDPlan match {
        case BroadcastHint(p) => true
        case p if sqlContext.conf.autoBroadcastJoinThreshold > 0 &&
                  p.statistics.sizeInBytes <= sqlContext.conf.autoBroadcastJoinThreshold => {
          LOGGER.info(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG,
            "canPushDownJoin statistics:" + p.statistics.sizeInBytes)
          true
        }
        case _ => false
      }
    }

    private def carbonScan(projectList: Seq[NamedExpression],
                           predicates: Seq[Expression],
                           relation: CarbonRelation,
                           detailQuery: Boolean = true) = {

      if (detailQuery == false) {
        val projectSet = AttributeSet(projectList.flatMap(_.references))
        CarbonRawCubeScan(
          projectSet.toSeq,
          relation,
          predicates)(sqlContext)
      }
      else {
        val projectSet = AttributeSet(projectList.flatMap(_.references))
        Project(projectList,
          CarbonRawCubeScan(projectSet.toSeq,
            relation,
            predicates)(sqlContext))

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
