package org.carbondata.hadoop.readsupport.impl;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.carbondata.hadoop.readsupport.CarbonReadSupport;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

/**
 * Created by root1 on 1/5/16.
 */
public class SparkRowReadSupportImpl implements CarbonReadSupport<InternalRow> {

  @Override public void intialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    //can intialize and generate schema here.
  }

  @Override public InternalRow readRow(Object[] data) {
    return new GenericInternalRow(data);
  }
}
