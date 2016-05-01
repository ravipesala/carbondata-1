package org.carbondata.hadoop.readsupport;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;

/**
 * Created by root1 on 1/5/16.
 */
public interface CarbonReadSupport<T> {

  /**
   * It can use [{@link CarbonColumn}] array to create its own schema to create its row.
   *
   * @param carbonColumns
   */
  public void intialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier);

  public T readRow(Object[] data);

}
