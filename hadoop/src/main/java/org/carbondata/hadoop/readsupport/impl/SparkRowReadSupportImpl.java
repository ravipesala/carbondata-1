package org.carbondata.hadoop.readsupport.impl;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

public class SparkRowReadSupportImpl extends AbstractDictionaryDecodedReadSupport<InternalRow> {

  @Override public void intialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    super.intialize(carbonColumns, absoluteTableIdentifier);
    //can intialize and generate schema here.
  }

  @Override public InternalRow readRow(Object[] data) {
    for (int i = 0; i < dictionaries.length; i++) {
      if (dictionaries[i] != null) {
        data[i] = dictionaries[i].getDictionaryValueForKey((int) data[i]);
      }
    }
    return new GenericInternalRow(data);
  }
}
