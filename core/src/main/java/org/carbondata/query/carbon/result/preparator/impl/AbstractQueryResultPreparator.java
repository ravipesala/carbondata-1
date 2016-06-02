package org.carbondata.query.carbon.result.preparator.impl;

import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.executor.impl.QueryExecutorProperties;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.BatchResult;
import org.carbondata.query.carbon.result.preparator.QueryResultPreparator;

public abstract class AbstractQueryResultPreparator<K, V> implements QueryResultPreparator<K, V> {

  /**
   * query properties
   */
  protected QueryExecutorProperties queryExecuterProperties;

  /**
   * query model
   */
  protected QueryModel queryModel;

  public AbstractQueryResultPreparator(QueryExecutorProperties executerProperties,
      QueryModel queryModel) {
    this.queryExecuterProperties = executerProperties;
    this.queryModel = queryModel;
  }

  protected void fillMeasureValueForAggGroupByQuery(QueryModel queryModel,
      Object[][] surrogateResult, int dimensionCount, int columnIndex, MeasureAggregator[] v) {
    int msrCount = queryModel.getQueryMeasures().size();
    for (int i = 0; i < msrCount; i++) {
      v[queryExecuterProperties.measureStartIndex + i] =
          ((MeasureAggregator) surrogateResult[dimensionCount
              + queryExecuterProperties.measureStartIndex + i][columnIndex]);
    }
  }

  protected Object[][] encodeToRows(Object[][] data) {
    if (data.length == 0) {
      return data;
    }
    Object[][] rData = new Object[data[0].length][data.length];
    int len = data.length;
    for (int i = 0; i < rData.length; i++) {
      for (int j = 0; j < len; j++) {
        rData[i][j] = data[j][i];
      }
    }
    return rData;
  }

  protected BatchResult getEmptyChunkResult(int size) {
    Object[][] row = new Object[size][1];
    BatchResult chunkResult = new BatchResult();
    chunkResult.setRows(row);
    return chunkResult;
  }

}
