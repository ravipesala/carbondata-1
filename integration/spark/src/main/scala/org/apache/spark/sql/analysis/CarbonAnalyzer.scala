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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis.{Analyzer, Catalog, FunctionRegistry}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.IntegerType

/**
 * Carbon Analyzer to add dictionary decoder.
 */
class CarbonAnalyzer(catalog: Catalog,
  registry: FunctionRegistry,
  conf: CatalystConf) extends Analyzer(catalog, registry, conf) {


  def executePlan(plan: LogicalPlan): LogicalPlan = {
    val executedPlan: LogicalPlan = super.execute(plan)
    if (!conf.asInstanceOf[CarbonSQLConf].pushComputation) {
      ResolveCarbonFunctions(executedPlan)
    } else {
      executedPlan
    }
  }

  /**
   * It does two jobs. 1. Change the datatype for dictionary encoded column 2. Add the dictionary
   * decoder plan.
   */
  object ResolveCarbonFunctions extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = {
      val relation = collectCarbonRelation(plan)
      transformCarbonPlan(plan, relation)
    }

    def transformCarbonPlan(plan: LogicalPlan, relation: CarbonDatasourceRelation): LogicalPlan = {
      var decoder = false
      plan.transform {
        case cd: CarbonDictionaryCatalystDecoder =>
          decoder = true
          cd
        case sort: Sort =>
          val sortExprs = sort.order.map { s =>
            s.transform {
              case attr: AttributeReference => updateDataType(attr, relation)
            }.asInstanceOf[SortOrder]
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryCatalystDecoder(relation, Sort(sortExprs, sort.global, sort.child))
          } else {
            Sort(sortExprs, sort.global, sort.child)
          }
        case agg: Aggregate =>
          val aggExps = agg.aggregateExpressions.map { aggExp =>
            aggExp.transform {
              case attr: AttributeReference => updateDataType(attr, relation)
            }
          }.asInstanceOf[Seq[NamedExpression]]
          val grpExps = agg.groupingExpressions.map { gexp =>
            gexp.transform {
              case attr: AttributeReference => updateDataType(attr, relation)
            }
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryCatalystDecoder(relation, Aggregate(grpExps, aggExps, agg.child))
          } else {
            Aggregate(grpExps, aggExps, agg.child)
          }
        case filter: Filter =>
          val filterExps = filter.condition transform {
            case attr: AttributeReference => updateDataType(attr, relation)
            case l: Literal => FakeCarbonCast(l, IntegerType)
          }
          if (!decoder) {
            decoder = true
            CarbonDictionaryCatalystDecoder(relation, Filter(filterExps, filter.child))
          } else {
            Filter(filterExps, filter.child)
          }
        case p: Project =>
          val prExps = p.projectList.map { prExp =>
            prExp.transform {
              case attr: AttributeReference => updateDataType(attr, relation)
            }
          }.asInstanceOf[Seq[NamedExpression]]
          if (!decoder) {
            decoder = true
            CarbonDictionaryCatalystDecoder(relation, Project(prExps, p.child))
          } else {
            Project(prExps, p.child)
          }
        case others => others
      }
    }

    /**
     * Update the attribute datatype with [IntegerType] if the carbon column is encoded with
     * dictionary.
     *
     * @param attr
     * @param relation
     * @return
     */
    private def updateDataType(attr: AttributeReference, relation: CarbonDatasourceRelation) = {
      relation.carbonRelation.metaData.dictionaryMap.get(attr.name) match {
        case Some(true) =>
          AttributeReference(attr.name, IntegerType, attr.nullable, attr.metadata)(attr.exprId,
            attr.qualifiers)
        case _ => attr
      }
    }

    // get the carbon relation from plan.
    def collectCarbonRelation(plan: LogicalPlan): CarbonDatasourceRelation = {
      var relation: CarbonDatasourceRelation = null
      plan collect {
        case LogicalRelation(carbonRelation: CarbonDatasourceRelation, _) =>
          relation = carbonRelation
      }
      relation
    }
  }

}
