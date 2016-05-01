package org.carbondata.integration.spark.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, GenericRow}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._
import org.apache.spark.sql.{PhysicalOperation1, Row}
import org.apache.spark.{Logging, Partition, TaskContext}
import org.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.carbondata.core.carbon.metadata.datatype.DataType
import org.carbondata.core.carbon.metadata.encoder.Encoding
import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.carbondata.integration.spark.util.CarbonScalaUtil
import org.carbondata.query.carbon.util.DataTypeUtil

/**
  * Created by root1 on 29/4/16.
  */
class CarbonDictionaryDecodeRDD(prev: RDD[Row],
                                plan: LogicalPlan,
                                schema: StructType,
                                storePath: String)
  extends RDD[(Row)](prev) with Logging {

  override def getPartitions: Array[Partition] = firstParent[Row].partitions

  val aliases = {
    val (_, _, _, aliases, _, _, _) =
      PhysicalOperation1.collectProjectsAndFilters(plan)
    aliases.map { a =>
      val name  = a._2 match {
        case attr:AttributeReference => attr.name
        case other => a._2.nodeName
      }
      (a._1.name, name)
    }.toMap
  }

  val carbonRelation = {
    PhysicalOperation1.collectRelation(plan).carbonRelation
  }

  val dictionaryColumnIds = {
    val carbonTable = carbonRelation.metaData.carbonTable
    val dictIds: Array[(String, DataType)] = schema.map(field => {
      val updatedName = aliases.get(field.name) match {
        case Some(name) => name
        case None => field.name
      }
      val carbonDimension =
        carbonTable.getDimensionByName(carbonTable.getFactTableName, updatedName);
      if (carbonDimension != null && carbonDimension.hasEncoding(Encoding.DICTIONARY)) {
        (carbonDimension.getColumnId, carbonDimension.getDataType)
      } else {
        (null, null)
      }
    }).toArray
    dictIds
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val carbonTable = carbonRelation.metaData.carbonTable
    val absoluteTableIdentifier = new AbsoluteTableIdentifier(storePath,
      new CarbonTableIdentifier(carbonTable.getDatabaseName, carbonTable.getFactTableName))
    val cacheProvider: CacheProvider = CacheProvider.getInstance
    val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
      cacheProvider
      .createCache(CacheType.FORWARD_DICTIONARY, absoluteTableIdentifier.getStorePath)
    val dicts: Seq[Dictionary] = getDictionary(absoluteTableIdentifier, forwardDictionaryCache)

    val iter = firstParent[Row].iterator(split, context)

    new Iterator[Row] {
      override final def hasNext: Boolean = iter.hasNext

      override final def next(): Row = {
        val row = iter.next().toSeq.toArray
        for (i <- 0 until dicts.length) {
          if (dicts(i) != null) {
            row(i) = DataTypeUtil
                     .getDataBasedOnDataType(dicts(i)
                       .getDictionaryValueForKey(row(i).asInstanceOf[Integer]),
                       dictionaryColumnIds(i)._2)
          }
        }
        new GenericRow(row)
      }
    }
  }

  private def getDictionary(ati: AbsoluteTableIdentifier,
                    cache: Cache[DictionaryColumnUniqueIdentifier, Dictionary]) = {
    val dicts: Seq[Dictionary] = dictionaryColumnIds.map { f =>
      if (f._1 != null) {
        cache.get(new DictionaryColumnUniqueIdentifier(
          ati.getCarbonTableIdentifier, f._1))
      } else {
        null
      }
    }
    dicts
  }

  def getUpdatedSchema: StructType = {
    val carbonTable = carbonRelation.metaData.carbonTable
    StructType(schema.map{field =>
      val updatedName = aliases.get(field.name) match {
        case Some(name) => name
        case None => field.name
      }
      val carbonDimension =
        carbonTable.getDimensionByName(carbonTable.getFactTableName, updatedName);
      if (carbonDimension != null && carbonDimension.hasEncoding(Encoding.DICTIONARY)) {
        StructField(field.name,
          CarbonScalaUtil.convertCarbonToSparkDataType(carbonDimension.getDataType),
          field.nullable,
          field.metadata)
      } else {
        field
      }
    })
  }
}
