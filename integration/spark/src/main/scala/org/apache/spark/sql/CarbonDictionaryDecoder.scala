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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
import org.apache.spark.sql.hive.CarbonMetastoreCatalog
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import org.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.carbondata.core.carbon.metadata.datatype.DataType
import org.carbondata.core.carbon.metadata.encoder.Encoding
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.carbondata.query.carbon.util.DataTypeUtil

/**
 * It decodes the data.
 *
 * @param relations
 * @param child
 * @param sqlContext
 */
case class CarbonDictionaryDecoder(relations: Map[String, CarbonDatasourceRelation],
  profile: CarbonProfile,
  aliasMap: Map[String, Attribute],
  child: SparkPlan)
  (@transient sqlContext: SQLContext)
  extends UnaryNode {


  override def otherCopyArgs: Seq[AnyRef] = sqlContext :: Nil

  override def output: Seq[Attribute] = {
    child.output.map { attr =>
      var attrReference = attr.asInstanceOf[AttributeReference]
      if (attrReference.qualifiers.size == 0) {
        attrReference = aliasMap.get(attrReference.name) match {
          case Some(attr: AttributeReference) => attr
          case _ => attrReference
        }
      }

      if (attrReference.qualifiers.size > 0) {
        val carbonTable: CarbonTable = relations.get(attrReference.qualifiers(0)).get.carbonRelation
                                       .metaData.carbonTable
        val carbonDimension = carbonTable
                              .getDimensionByName(carbonTable.getFactTableName, attr.name);
        if (carbonDimension != null && carbonDimension.getEncoder.contains(Encoding.DICTIONARY) &&
          canBeDecoded(attr)) {
          val d = attr.asInstanceOf[AttributeReference]
          val a = AttributeReference(d.name,
            convertCarbonToSparkDataType(carbonDimension.getDataType),
            d.nullable,
            d.metadata)(d.exprId,
            d.qualifiers).asInstanceOf[Attribute]
          a.resolved
          a
        } else {
          attr
        }
      } else {
        attr
      }
    }
  }

  def canBeDecoded(attr: Attribute): Boolean = {
    profile match {
      case ip: IncludeProfile if ip.attributes.size > 0 =>
        ip.attributes.filter(a => a.name.equals(attr.name)).size>0
      case ep: ExcludeProfile =>
        !(ep.attributes.filter(a => a.name.equals(attr.name)).size>0)
      case _ => true
    }
  }

  def convertCarbonToSparkDataType(dataType: DataType): types.DataType = {
    dataType match {
      case DataType.STRING => StringType
      case DataType.INT => IntegerType
      case DataType.LONG => LongType
      case DataType.DOUBLE => DoubleType
      case DataType.BOOLEAN => BooleanType
      case DataType.DECIMAL => DecimalType.DoubleDecimal
      case DataType.TIMESTAMP => TimestampType
    }
  }

  val getDictionaryColumnIds = {
    val attributes = child.output
    val dictIds: Array[(String, String, DataType)] = attributes.map(attr => {
      var attrReference = attr.asInstanceOf[AttributeReference]
      if (attrReference.qualifiers.size == 0) {
        attrReference = aliasMap.get(attrReference.name) match {
          case Some(attr: AttributeReference) => attr
          case _ => attrReference
        }
      }
      if (attrReference.qualifiers.size > 0) {
        val carbonTable = relations.get(attrReference.qualifiers(0))
                          .get.carbonRelation.metaData.carbonTable
        val carbonDimension =
          carbonTable.getDimensionByName(carbonTable.getFactTableName, attr.name);
        if (carbonDimension != null && carbonDimension.hasEncoding(Encoding.DICTIONARY) &&
          canBeDecoded(attr)) {
          (attrReference.qualifiers(0), carbonDimension.getColumnId, carbonDimension.getDataType)
        } else {
          (null, null, null)
        }
      } else {
        (null, null, null)
      }
    }).toArray
    dictIds
  }

  override def doExecute(): RDD[InternalRow] = {
    attachTree(this, "execute") {
      val storePath = sqlContext.catalog.asInstanceOf[CarbonMetastoreCatalog].storePath
      val absoluteTableIdentifiers = relations.map { relation =>
        val carbonTable = relation._2.carbonRelation.metaData.carbonTable
        (relation._1, new AbsoluteTableIdentifier(storePath,
          new CarbonTableIdentifier(carbonTable.getDatabaseName, carbonTable.getFactTableName)))
      }

      val dataTypes = child.output.map { attr => attr.dataType }
      child.execute().mapPartitions { iter =>
        val cacheProvider: CacheProvider = CacheProvider.getInstance
        val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
          cacheProvider
          .createCache(CacheType.FORWARD_DICTIONARY, storePath)
        val dicts: Seq[Dictionary] = getDictionary(absoluteTableIdentifiers, forwardDictionaryCache)
        new Iterator[InternalRow] {
          override final def hasNext: Boolean = iter.hasNext

          override final def next(): InternalRow = {
            val row: InternalRow = iter.next()
            val data = row.toSeq(dataTypes).toArray
            for (i <- 0 until data.length) {
              if (dicts(i) != null) {
                data(i) = toType(DataTypeUtil
                                 .getDataBasedOnDataType(dicts(i)
                                 .getDictionaryValueForKey(data(i)
                                 .asInstanceOf[Integer]),
                                  getDictionaryColumnIds(i)._3))
              }
            }
            new GenericMutableRow(data)
          }
        }
      }

    }
  }

  private def toType(obj: Any): Any = {
    obj match {
      case s: String => UTF8String.fromString(s)
      case _ => obj
    }
  }

  private def getDictionary(atiMap: Map[String, AbsoluteTableIdentifier],
    cache: Cache[DictionaryColumnUniqueIdentifier, Dictionary]) = {
    val dicts: Seq[Dictionary] = getDictionaryColumnIds.map { f =>
      if (f._2 != null) {
        cache.get(new DictionaryColumnUniqueIdentifier(
          atiMap.get(f._1).get.getCarbonTableIdentifier,
          f._2))
      } else {
        null
      }
    }
    dicts
  }
}
