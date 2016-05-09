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
import org.apache.spark.sql.catalyst.{CatalystConf, CatalystTypeConverters}
import org.apache.spark.sql.catalyst.CatalystTypeConverters._
import org.apache.spark.sql.catalyst.expressions.{AggregateExpression, Attribute, _}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.IntegerType

/**
 * Carbon Optimizer to add dictionary decoder.
 */
class CarbonOptimizer(optimizer: Optimizer, conf: CatalystConf) extends Optimizer {

  val batches = Nil

  override def execute(plan: LogicalPlan): LogicalPlan = {
    val relations = collectCarbonRelation(plan)
    val executedPlan: LogicalPlan = optimizer.execute(plan)
    if (!conf.asInstanceOf[CarbonSQLConf].pushComputation) {
      new ResolveCarbonFunctions(relations)(executedPlan)
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
     *    be decoded just before that plan executes.
     * 2. Plan starts transforms
     * @param plan
     * @param relations
     * @return
     */
    def transformCarbonPlan(plan: LogicalPlan,
      relations: Map[String, CarbonDatasourceRelation]): LogicalPlan = {
      var decoder = false
      val attrsOnJoin = new util.HashSet[AttributeReference]
      val attrsOndimAggs = new util.HashSet[AttributeReference]
      val attrsOnConds = new util.HashSet[AttributeReference]
      val aliasMap = new util.HashMap[String, Attribute]()
      collectAttributesNeedDecode(plan, attrsOnJoin, attrsOndimAggs, attrsOnConds)
      val allAttrsNotDecode = new util.HashSet[AttributeReference]
      allAttrsNotDecode.addAll(attrsOndimAggs)
      allAttrsNotDecode.addAll(attrsOnJoin)
      allAttrsNotDecode.addAll(attrsOnConds)
      val transFormedPlan =
        plan transformDown {
          case cd: CarbonDictionaryCatalystDecoder =>
            decoder = true
            cd
          case sort: Sort =>
            val sortExprs = sort.order.map { s =>
              s.transform {
                case attr: AttributeReference => updateDataType(attr, relations, allAttrsNotDecode)
              }.asInstanceOf[SortOrder]
            }
            if (!decoder) {
              decoder = true
              CarbonDictionaryCatalystDecoder(relations,
                IncludeProfile(Seq()), Map(),
                Sort(sortExprs, sort.global, sort.child))
            } else {
              Sort(sortExprs, sort.global, sort.child)
            }
          case agg: Aggregate if !agg.child.isInstanceOf[CarbonDictionaryCatalystDecoder] =>
            val aggExps = agg.aggregateExpressions.map { aggExp =>
              aggExp.transform {
                case attr: AttributeReference => updateDataType(attr, relations, allAttrsNotDecode)
              }
            }.asInstanceOf[Seq[NamedExpression]]

            val grpExps = agg.groupingExpressions.map { gexp =>
              gexp.transform {
                case attr: AttributeReference => updateDataType(attr, relations, allAttrsNotDecode)
              }
            }
            var child = agg.child
            if (attrsOndimAggs.size() > 0) {
              // Filter out already decoded attr during join operation
              val filteredAggs = attrsOndimAggs.asScala
                                 .filterNot(attr => attrsOnJoin.contains(attr))
              filteredAggs.map(allAttrsNotDecode.remove)
              if (filteredAggs.size > 0) {
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
              case attr: AttributeReference => updateDataType(attr, relations, allAttrsNotDecode)
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
                if (qualifierPresence(j.left, attr.qualifiers(0))) {
                  leftCondAttrs += attr
                } else {
                  rightCondAttrs += attr
                }
              }
              var leftPlan = j.left
              var rightPlan = j.right
              if (leftCondAttrs.size > 0 ) {
                leftPlan = CarbonDictionaryCatalystDecoder(relations,
                  IncludeProfile(leftCondAttrs),
                  Map(),
                  j.left)
              }
              if (rightCondAttrs.size > 0 &&
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
          case p: Project if relations.size > 0 =>
            val prExps = p.projectList.map { prExp =>
              prExp.transform {
                case attr: AttributeReference => updateDataType(attr, relations, allAttrsNotDecode)
                case alias @ Alias(attr: Attribute, name: String) =>
                  aliasMap.put(name, attr)
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
          case others => others
        }
      attrsOnJoin.addAll(attrsOndimAggs)
      attrsOnJoin.addAll(attrsOnConds)
      // It means join is present in the plan.
      if (attrsOnJoin.size() > 0) {
        // transform the plan again to exclude the already decoded dictionary values
        transFormedPlan transform {
          case cd: CarbonDictionaryCatalystDecoder =>
            cd.profile match {
              case ip: IncludeProfile if ip.attributes.size == 0 =>
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
      } else {
        transFormedPlan
      }
    }

    private def collectAttributesNeedDecode(plan: LogicalPlan,
      attrsOnJoin: util.HashSet[AttributeReference],
      attrsOndimAggs: util.HashSet[AttributeReference],
      attrsOnConds: util.HashSet[AttributeReference]) {

      def collectProjectAndConditions(projectList: Seq[NamedExpression],
        condition: Expression) {
        projectList.map { p =>
          p match {
            case attr: AttributeReference =>
            case others =>
              others.collect {
                case attr: AttributeReference => attrsOnConds.add(attr)
              }
          }
        }
        selectFilters(Seq(condition), attrsOnConds)
      }

      plan transform {
        case p@Project(projectList, Filter(condition, child)) =>
          collectProjectAndConditions(projectList, condition)
          p
        case f@Filter(condition, Project(projectList, child)) =>
          collectProjectAndConditions(projectList, condition)
          f
        case agg: Aggregate =>
          agg.aggregateExpressions.map { aggExp =>
            aggExp.transform {
              case aggExp: AggregateExpression =>
                collectDimensionAggregates(aggExp, attrsOndimAggs)
                aggExp
            }
          }
          agg
        case j: Join =>
          j.condition match {
            case Some(expression) =>
              expression.collect {
                case attr: AttributeReference if isDictionaryEncoded(attr, relations) =>
                  attrsOnJoin.add(attr)
              }
            case _ =>
          }
          j
        case others => others
      }
    }

    // Collect aggregates on dimensions so that we can add decoder to it.
    private def collectDimensionAggregates(aggExp: AggregateExpression,
      attrsOndimAggs: util.HashSet[AttributeReference]) {
      aggExp collect {
        case attr: AttributeReference if attr.qualifiers.size > 0 =>
          relations.get(attr.qualifiers(0)) match {
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

    /**
     * Update the attribute datatype with [IntegerType] if the carbon column is encoded with
     * dictionary.
     *
     * @param attr
     * @param relations
     * @return
     */
    private def updateDataType(attr: AttributeReference,
      relations: Map[String, CarbonDatasourceRelation],
      allAttrsNotDecode: util.HashSet[AttributeReference]) = {
      if (attr.qualifiers.size > 0) {
        relations.get(attr.qualifiers(0)) match {
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
      } else {
        attr
      }
    }

    private def isDictionaryEncoded(attr: AttributeReference,
      relations: Map[String, CarbonDatasourceRelation]): Boolean = {
      relations.get(attr.qualifiers(0)) match {
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
      val relations = plan collect {
        case Project(projectList, child) =>
          projectList.asInstanceOf[Seq[AttributeReference]].map { attr =>
            attr.qualifiers.map { qual =>
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
    val relations = plan collect {
      case Subquery(alias, LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)) =>
        (alias, carbonRelation)
    }
    relations.toMap
  }

  def selectFilters(filters: Seq[Expression],
    attrList: java.util.HashSet[AttributeReference]): Unit = {
    def translate(predicate: Expression): Option[sources.Filter] = predicate match {
      case EqualTo(a: Attribute, FakeCarbonCast(Literal(v, t), b)) =>
        Some(sources.EqualTo(a.name, convertToScala(v, t)))
      case EqualTo(FakeCarbonCast(Literal(v, t), b), a: Attribute) =>
        Some(sources.EqualTo(a.name, convertToScala(v, t)))

      // Because we only convert In to InSet in Optimizer when there are more than certain
      // items. So it is possible we still get an In expression here that needs to be pushed
      // down.
      case In(a: Attribute, list) if !list.exists(!_.isInstanceOf[Literal]) =>
        val hSet = list.map(e => e.eval(EmptyRow))
        val toScala = CatalystTypeConverters.createToScalaConverter(a.dataType)
        Some(sources.In(a.name, hSet.toArray.map(toScala)))

      case And(left, right) =>
        (translate(left) ++ translate(right)).reduceOption(sources.And)

      case Or(left, right) =>
        for {
          leftFilter <- translate(left)
          rightFilter <- translate(right)
        } yield sources.Or(leftFilter, rightFilter)

      case Not(child) =>
        translate(child).map(sources.Not)

      case others =>
        others.collect {
          case attr: AttributeReference =>
            attrList.add(attr)
        }
        None
    }

    filters.flatMap(translate).toArray
  }

}
