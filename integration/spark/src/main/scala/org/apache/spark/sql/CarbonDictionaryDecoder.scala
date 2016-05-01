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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
import org.apache.spark.sql.hive.CarbonMetastoreCatalog
import org.apache.spark.sql.types._

import org.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.carbondata.core.carbon.metadata.datatype.DataType
import org.carbondata.core.carbon.metadata.encoder.Encoding
import org.carbondata.query.carbon.util.DataTypeUtil

 /**
  * It decodes the data.
  * @param relation
  * @param child
  * @param sqlContext
  */
case class CarbonDictionaryDecoder(relation: CarbonRelation,
                                   child: SparkPlan)
                                  (@transient sqlContext: SQLContext)
  extends UnaryNode {


  override def otherCopyArgs: Seq[AnyRef] = sqlContext :: Nil

  // HACK: Generators don't correctly preserve their output through serializations so we grab
  // out child's output attributes statically here.

  override def output: Seq[Attribute] = {
    val carbonTable = relation.metaData.carbonTable
    child.output.map { attr =>
      val carbonDimension = carbonTable.getDimensionByName(carbonTable.getFactTableName, attr.name);
      if (carbonDimension != null && carbonDimension.getEncoder.contains(Encoding.DICTIONARY)) {
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
    val carbonTable = relation.metaData.carbonTable
    val attributes = child.output
    val dictIds: Array[(String, DataType)] = attributes.map(attr => {
        val carbonDimension =
          carbonTable.getDimensionByName(carbonTable.getFactTableName, attr.name);
        if (carbonDimension != null && carbonDimension.hasEncoding(Encoding.DICTIONARY)) {
          (carbonDimension.getColumnId, carbonDimension.getDataType)
        }
        (null, null)
      }).toArray
    dictIds
  }

  override def doExecute(): RDD[InternalRow] = {
    attachTree(this, "execute") {
      val carbonTable = relation.metaData.carbonTable
      val catalog = sqlContext.catalog.asInstanceOf[CarbonMetastoreCatalog]
      val absoluteTableIdentifier = new AbsoluteTableIdentifier(catalog.storePath,
        new CarbonTableIdentifier(carbonTable.getDatabaseName, carbonTable.getFactTableName))
      val dataTypes = child.output.map { attr => attr.dataType }
      child.execute().mapPartitions { iter =>
        val cacheProvider: CacheProvider = CacheProvider.getInstance
        val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
          cacheProvider
          .createCache(CacheType.FORWARD_DICTIONARY, absoluteTableIdentifier.getStorePath)
        val dicts: Seq[Dictionary] = getDictionary(absoluteTableIdentifier, forwardDictionaryCache)
        new Iterator[InternalRow] {
          override final def hasNext: Boolean = iter.hasNext

          override final def next(): InternalRow = {
            val row: InternalRow = iter.next()
            val data = row.toSeq(dataTypes).toArray
            for (i <- 0 until data.length) {
              if (dicts(i) != null) {
                data(i) = DataTypeUtil
                 .getDataBasedOnDataType(dicts(i)
                    .getDictionaryValueForKey(data(i).asInstanceOf[Integer]),
                            getDictionaryColumnIds(i)._2)
              }
            }
            new GenericMutableRow(data)
          }
        }
      }

    }
  }

  private def getDictionary(ati: AbsoluteTableIdentifier,
                    cache: Cache[DictionaryColumnUniqueIdentifier, Dictionary]) = {
    val dicts: Seq[Dictionary] = getDictionaryColumnIds.map { f =>
      if (f._1 != null) {
        cache.get(new DictionaryColumnUniqueIdentifier(
          ati.getCarbonTableIdentifier,
          f._1))
      }
      null
    }
    dicts
  }
}
