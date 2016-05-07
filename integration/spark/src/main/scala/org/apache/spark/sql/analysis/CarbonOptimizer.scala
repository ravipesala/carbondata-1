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

package org.apache.spark.sql.analysis

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions._
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

  object DummyRule extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan
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

    def transformCarbonPlan(plan: LogicalPlan,
      relations: Map[String, CarbonDatasourceRelation]): LogicalPlan = {
      var decoder = false
      val attrsOnJoin = new util.HashSet[AttributeReference]
      val transFormedPlan =
      plan transform {
        case cd: CarbonDictionaryCatalystDecoder =>
          decoder = true
          cd
        case sort: Sort =>
          val sortExprs = sort.order.map { s =>
            s.transform {
              case attr: AttributeReference => updateDataType(attr, relations)
            }.asInstanceOf[SortOrder]
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryCatalystDecoder(relations,
              Seq(),
              false,
              Sort(sortExprs, sort.global, sort.child))
          } else {
            Sort(sortExprs, sort.global, sort.child)
          }
        case agg: Aggregate =>
          val aggExps = agg.aggregateExpressions.map { aggExp =>
            aggExp.transform {
              case attr: AttributeReference => updateDataType(attr, relations)
            }
          }.asInstanceOf[Seq[NamedExpression]]
          val grpExps = agg.groupingExpressions.map { gexp =>
            gexp.transform {
              case attr: AttributeReference => updateDataType(attr, relations)
            }
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryCatalystDecoder(relations,
              Seq(),
              false,
              Aggregate(grpExps, aggExps, agg.child))
          } else {
            Aggregate(grpExps, aggExps, agg.child)
          }
        case filter: Filter =>
          val filterExps = filter.condition transform {
            case attr: AttributeReference => updateDataType(attr, relations)
            case l: Literal => FakeCarbonCast(l, IntegerType)
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryCatalystDecoder(relations,
              Seq(),
              false,
              Filter(filterExps, filter.child))
          } else {
            Filter(filterExps, filter.child)
          }
        case j: Join =>
          j.condition match {
            case Some(expression) =>
              expression.collect {
                case attr: AttributeReference if isDictionaryEncoded(attr, relations) =>
                  attrsOnJoin.add(attr)
              }
            case _ =>
          }
          val leftCondAttrs = new ArrayBuffer[AttributeReference]
          val rightCondAttrs = new ArrayBuffer[AttributeReference]
          if (attrsOnJoin.size() > 0) {
            attrsOnJoin.asScala.map { attr =>
              if (qualifierPresence(j.left, attr.qualifiers(0))) {
                leftCondAttrs += attr
              } else {
                rightCondAttrs += attr
              }
            }
            var leftPlan = j.left
            var rightPlan = j.right
            if (leftCondAttrs.size > 0) {
              leftPlan = CarbonDictionaryCatalystDecoder(relations, leftCondAttrs, true, j.left)
            }
            if (rightCondAttrs.size > 0) {
              rightPlan = CarbonDictionaryCatalystDecoder(relations, rightCondAttrs, true, j.right)
            }
            if (!decoder) {
              decoder = true
              CarbonDictionaryCatalystDecoder(relations,
                attrsOnJoin.asScala.toSeq,
                false,
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
              case attr: AttributeReference => updateDataType(attr, relations)
            }
          }.asInstanceOf[Seq[NamedExpression]]
          if (!decoder) {
            decoder = true
            CarbonDictionaryCatalystDecoder(relations, Seq(), false, Project(prExps, p.child))
          } else {
            Project(prExps, p.child)
          }
        case others => others
      }
      // It means join is present in the plan.
      if(attrsOnJoin.size() > 0) {
        // transform the plan again to exclude the already decoded dictionary values
        transFormedPlan transform {
          case cd: CarbonDictionaryCatalystDecoder if cd.attributes.size  == 0 =>
            CarbonDictionaryCatalystDecoder(relations, attrsOnJoin.asScala.toSeq, false, cd.child)
        }
      } else {
        transFormedPlan
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
      relations: Map[String, CarbonDatasourceRelation]) = {
      relations.get(attr.qualifiers(0)) match {
        case Some(cd: CarbonDatasourceRelation) =>
          cd.carbonRelation.metaData.dictionaryMap.get(attr.name) match {
            case Some(true) =>
              AttributeReference(attr.name, IntegerType, attr.nullable, attr.metadata)(attr.exprId,
                attr.qualifiers)
            case _ => attr
          }
        case _ => attr
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

}
