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

package org.apache.spark.sql.optimizer

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions.{AggregateExpression, Attribute, _}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.IntegerType

import org.carbondata.spark.CarbonFilters

/**
 * Carbon Optimizer to add dictionary decoder.
 */
class CarbonOptimizer(optimizer: Optimizer, conf: CatalystConf) extends Optimizer {

  val batches = Nil

  override def execute(plan: LogicalPlan): LogicalPlan = {
    val executedPlan: LogicalPlan = optimizer.execute(plan)
    if (!conf.asInstanceOf[CarbonSQLConf].pushComputation) {
      val relations = collectCarbonRelation(plan)
      if (relations.nonEmpty) {
        new ResolveCarbonFunctions(relations)(executedPlan)
      } else {
        executedPlan
      }
    } else {
      executedPlan
    }
  }

  /**
   * It does two jobs. 1. Change the datatype for dictionary encoded column 2. Add the dictionary
   * decoder plan.
   */
  class ResolveCarbonFunctions(relations: Map[String, CarbonDatasourceRelation]) extends
    Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = {
      transformCarbonPlan(plan, relations)
    }

    /**
     * Steps for changing the plan.
     * 1. It finds out the join condition columns and dimension aggregate columns which are need to
     * be decoded just before that plan executes.
     * 2. Plan starts transform by adding the decoder to the plan where it needs the decoded data
     * like dimension aggregate columns decoder under aggregator and join condition decoder under
     * join children.
     */
    def transformCarbonPlan(plan: LogicalPlan,
        relations: Map[String, CarbonDatasourceRelation]): LogicalPlan = {
      var decoder = false
      val attrsOnJoin = new util.HashSet[AttributeReference]
      val attrsOndimAggs = new util.HashSet[AttributeReference]
      val attrsOnConds = new util.HashSet[AttributeReference]
      val attrsOnSort = new util.HashSet[AttributeReference]
      val aliasMap = new util.HashMap[String, String]()
      collectInformationOnAttributes(plan, attrsOnJoin, attrsOndimAggs,
        attrsOnConds, attrsOnSort, aliasMap)
      val allAttrsNotDecode = new util.HashSet[AttributeReference]
      allAttrsNotDecode.addAll(attrsOndimAggs)
      allAttrsNotDecode.addAll(attrsOnJoin)
      allAttrsNotDecode.addAll(attrsOnConds)
      allAttrsNotDecode.addAll(attrsOnSort)
      val transFormedPlan =
        plan transformDown {
          case cd: CarbonDictionaryCatalystDecoder =>
            decoder = true
            cd
          case sort: Sort if !sort.child.isInstanceOf[CarbonDictionaryCatalystDecoder] =>
            val sortExprs = sort.order.map { s =>
              s.transform {
                case attr: AttributeReference =>
                  updateDataType(attr, relations, allAttrsNotDecode, aliasMap)
              }.asInstanceOf[SortOrder]
            }
            var child = sort.child
            if (attrsOnSort.size() > 0 && !child.isInstanceOf[Sort]) {
              val sortAggs = attrsOnSort.asScala
                .filterNot(attr => (attrsOnJoin.contains(attr) || attrsOndimAggs.contains(attr)))
              sortAggs.map(allAttrsNotDecode.remove)
              if (sortAggs.nonEmpty) {
                child = CarbonDictionaryCatalystDecoder(relations,
                  IncludeProfile(sortAggs.toSeq), Map(),
                  sort.child)
              }
            }
            if (!decoder) {
              decoder = true
              CarbonDictionaryCatalystDecoder(relations,
                IncludeProfile(Seq()), Map(),
                Sort(sortExprs, sort.global, child))
            } else {
              Sort(sortExprs, sort.global, child)
            }
          case agg: Aggregate if !agg.child.isInstanceOf[CarbonDictionaryCatalystDecoder] =>
            val aggExps = agg.aggregateExpressions.map { aggExp =>
              aggExp.transform {
                case attr: AttributeReference =>
                  updateDataType(attr, relations, allAttrsNotDecode, aliasMap)
              }
            }.asInstanceOf[Seq[NamedExpression]]

            val grpExps = agg.groupingExpressions.map { gexp =>
              gexp.transform {
                case attr: AttributeReference =>
                  updateDataType(attr, relations, allAttrsNotDecode, aliasMap)
              }
            }
            var child = agg.child
            // Incase if the child also aggregate then push down decoder to child
            if (attrsOndimAggs.size() > 0 && !child.isInstanceOf[Aggregate]) {
              // Filter out already decoded attr during join operation
              val filteredAggs = attrsOndimAggs.asScala
                .filterNot(attr => attrsOnJoin.contains(attr))
              filteredAggs.map(allAttrsNotDecode.remove)
              if (filteredAggs.nonEmpty) {
                child = CarbonDictionaryCatalystDecoder(relations,
                  IncludeProfile(filteredAggs.toSeq), Map(),
                  agg.child)
              }
            }
            if (!decoder) {
              decoder = true
              CarbonDictionaryCatalystDecoder(relations,
                IncludeProfile(Seq()), Map(),
                Aggregate(grpExps, aggExps, child))
            } else {
              Aggregate(grpExps, aggExps, child)
            }
          case filter: Filter =>
            val filterExps = filter.condition transform {
              case attr: AttributeReference =>
                updateDataType(attr, relations, allAttrsNotDecode, aliasMap)
              case l: Literal => FakeCarbonCast(l, l.dataType)
            }
            if (!decoder) {
              decoder = true
              CarbonDictionaryCatalystDecoder(relations,
                IncludeProfile(Seq()), Map(),
                Filter(filterExps, filter.child))
            } else {
              Filter(filterExps, filter.child)
            }
          case j: Join
            if !(j.left.isInstanceOf[CarbonDictionaryCatalystDecoder] ||
                 j.right.isInstanceOf[CarbonDictionaryCatalystDecoder]) =>

            val leftCondAttrs = new ArrayBuffer[AttributeReference]
            val rightCondAttrs = new ArrayBuffer[AttributeReference]
            if (attrsOnJoin.size() > 0) {
              attrsOnJoin.asScala.map(allAttrsNotDecode.remove)
              attrsOnJoin.asScala.map { attr =>
                if (qualifierPresence(j.left, attr.qualifiers.head)) {
                  leftCondAttrs += attr
                } else {
                  rightCondAttrs += attr
                }
              }
              var leftPlan = j.left
              var rightPlan = j.right
              if (leftCondAttrs.nonEmpty) {
                leftPlan = CarbonDictionaryCatalystDecoder(relations,
                  IncludeProfile(leftCondAttrs),
                  Map(),
                  j.left)
              }
              if (rightCondAttrs.nonEmpty &&
                  !rightPlan.isInstanceOf[CarbonDictionaryCatalystDecoder]) {
                rightPlan = CarbonDictionaryCatalystDecoder(relations,
                  IncludeProfile(rightCondAttrs), Map(),
                  j.right)
              }
              if (!decoder) {
                decoder = true
                CarbonDictionaryCatalystDecoder(relations,
                  ExcludeProfile(attrsOnJoin.asScala.toSeq), Map(),
                  Join(leftPlan, rightPlan, j.joinType, j.condition))
              } else {
                Join(leftPlan, rightPlan, j.joinType, j.condition)
              }
            } else {
              j
            }
          case p: Project if relations.nonEmpty =>
            val prExps = p.projectList.map { prExp =>
              prExp.transform {
                case attr: AttributeReference =>
                  updateDataType(attr, relations, allAttrsNotDecode, aliasMap)
                case alias@Alias(attr: Attribute, name: String)
                  if aliasMap.get(attr.exprId.id.toString) == null =>
                  aliasMap.put(attr.exprId.id.toString, attr.qualifiers.head)
                  alias
              }
            }.asInstanceOf[Seq[NamedExpression]]
            if (!decoder) {
              decoder = true
              CarbonDictionaryCatalystDecoder(relations,
                IncludeProfile(Seq()),
                Map(),
                Project(prExps, p.child))
            } else {
              Project(prExps, p.child)
            }
          case l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _) =>
            if (!decoder) {
              decoder = true
              CarbonDictionaryCatalystDecoder(relations,
                IncludeProfile(Seq()),
                Map(), l)
            } else {
              l
            }
          case others => others
        }
      attrsOnJoin.addAll(attrsOndimAggs)
      attrsOnJoin.addAll(attrsOnConds)
      attrsOnJoin.addAll(attrsOnSort)
      // It means join is present in the plan.
      // transform the plan again to exclude the already decoded dictionary values
      transFormedPlan transform {
        case cd: CarbonDictionaryCatalystDecoder =>
          cd.profile match {
            case ip: IncludeProfile if ip.attributes.isEmpty && attrsOnJoin.size() > 0 =>
              CarbonDictionaryCatalystDecoder(relations,
                ExcludeProfile(attrsOnJoin.asScala.toSeq),
                aliasMap.asScala.toMap,
                cd.child)
            case _ => CarbonDictionaryCatalystDecoder(relations,
              cd.profile,
              aliasMap.asScala.toMap,
              cd.child)
          }
        case other => other
      }
    }

    private def collectInformationOnAttributes(plan: LogicalPlan,
        attrsOnJoin: util.HashSet[AttributeReference],
        attrsOndimAggs: util.HashSet[AttributeReference],
        attrsOnConds: util.HashSet[AttributeReference],
        attrsOnSort: util.HashSet[AttributeReference],
        aliasMap: util.HashMap[String, String]) {

      plan transformUp {
        case po@PhysicalOperation(projectList, predicates,
        l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)) =>
          collectProjectsAndConditions(po)
          po
        case agg: Aggregate =>
          agg.aggregateExpressions.map { aggExp =>
            aggExp.transform {
              case aggExp: AggregateExpression =>
                collectDimensionAggregates(aggExp, attrsOndimAggs, aliasMap)
                aggExp
            }
          }
          agg
        case j: Join =>
          j.condition match {
            case Some(expression) =>
              expression.collect {
                case attr: AttributeReference if isDictionaryEncoded(attr, relations, aliasMap) =>
                  attrsOnJoin.add(attr)
              }
            case _ =>
          }
          j
        case sort: Sort =>
          sort.order.map { s =>
            s.collect {
              case attr: AttributeReference if isDictionaryEncoded(attr, relations, aliasMap) =>
                attrsOnSort.add(attr)
            }
          }
          sort
      }

      def collectProjectsAndConditions(plan: LogicalPlan): Unit = {
        plan match {
          case PhysicalOperation(projectList, predicates,
          l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)) =>
            collectProjectAndConditions(projectList, predicates, carbonRelation)
            projectList.flatMap(_.references).map { attr =>
              aliasMap.put(attr.exprId.id.toString,
                carbonRelation.carbonRelation.tableName.toLowerCase)
            }
            predicates.flatMap(_.references).map { attr =>
              aliasMap.put(attr.exprId.id.toString,
                carbonRelation.carbonRelation.tableName.toLowerCase)
            }
          case _ =>
        }
      }

      def collectProjectAndConditions(projectList: Seq[NamedExpression],
          condition: Seq[Expression],
          carbonRelation: CarbonDatasourceRelation) {

        projectList.map {
          case attr: AttributeReference =>
          case Alias(attr: AttributeReference, _) =>
          case others =>
            others.collect {
              case attr: AttributeReference =>
                carbonRelation.carbonRelation.metaData.dictionaryMap.get(attr.name) match {
                  case Some(true) => attrsOnConds.add(attr)
                  case _ =>
                }
            }
        }
        CarbonFilters.selectFilters(condition, attrsOnConds)
      }
    }

    // Collect aggregates on dimensions so that we can add decoder to it.
    private def collectDimensionAggregates(aggExp: AggregateExpression,
        attrsOndimAggs: util.HashSet[AttributeReference],
        aliasMap: util.HashMap[String, String]) {
      aggExp collect {
        case attr: AttributeReference =>
          var qualifier: String = null
          if (attr.qualifiers.nonEmpty) {
            relations.get(attr.qualifiers.head) match {
              case Some(relation) => qualifier = attr.qualifiers.head
              case _ => qualifier = aliasMap.get(attr.exprId.id.toString)
            }
          } else {
            qualifier = aliasMap.get(attr.exprId.id.toString)
          }
          if (qualifier != null) {
            relations.get(qualifier) match {
              case Some(cd: CarbonDatasourceRelation) =>
                cd.carbonRelation.metaData.dictionaryMap.get(attr.name) match {
                  case Some(true) =>
                    attrsOndimAggs.add(attr)
                  case _ =>
                }
              case _ =>
            }
          }

      }
    }

    /**
     * Update the attribute datatype with [IntegerType] if the carbon column is encoded with
     * dictionary.
     *
     */
    private def updateDataType(attr: AttributeReference,
        relations: Map[String, CarbonDatasourceRelation],
        allAttrsNotDecode: util.HashSet[AttributeReference],
        aliasMap: util.HashMap[String, String]) = {
      relations.get(aliasMap.get(attr.exprId.id.toString)) match {
        case Some(cd: CarbonDatasourceRelation) =>
          cd.carbonRelation.metaData.dictionaryMap.get(attr.name) match {
            case Some(true) if !allAttrsNotDecode.contains(attr) =>
              AttributeReference(attr.name,
                IntegerType,
                attr.nullable,
                attr.metadata)(attr.exprId, attr.qualifiers)
            case _ => attr
          }
        case _ => attr
      }
    }

    private def isDictionaryEncoded(attr: AttributeReference,
        relations: Map[String, CarbonDatasourceRelation],
        aliasMap: util.HashMap[String, String]): Boolean = {

      var qualifier: String = null
      if (attr.qualifiers.nonEmpty) {
        relations.get(attr.qualifiers.head) match {
          case Some(relation) => qualifier = attr.qualifiers.head
          case _ => qualifier = aliasMap.get(attr.exprId.id.toString)
        }
      } else {
        qualifier = aliasMap.get(attr.exprId.id.toString)
      }

      relations.get(qualifier) match {
        case Some(cd: CarbonDatasourceRelation) =>
          cd.carbonRelation.metaData.dictionaryMap.get(attr.name) match {
            case Some(true) => true
            case _ => false
          }
        case _ => false
      }
    }

    def qualifierPresence(plan: LogicalPlan, qualifier: String): Boolean = {
      var present = false
      plan collect {
        case Project(projectList, child) =>
          projectList.asInstanceOf[Seq[AttributeReference]].foreach { attr =>
            attr.qualifiers.foreach { qual =>
              if (qual.equals(qualifier)) {
                present = true
              }
            }
          }
      }
      present
    }
  }

  // get the carbon relation from plan.
  def collectCarbonRelation(plan: LogicalPlan): Map[String, CarbonDatasourceRelation] = {
    val map = new ArrayBuffer[(String, CarbonDatasourceRelation)]()
    plan collect {
      case Subquery(alias, LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)) =>
        map += ((alias, carbonRelation))
        map += ((carbonRelation.carbonRelation.tableName.toLowerCase, carbonRelation))
    }
    map.toMap
  }
}
