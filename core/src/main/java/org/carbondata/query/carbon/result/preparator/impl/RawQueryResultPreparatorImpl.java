package org.carbondata.query.carbon.result.preparator.impl;

import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.executor.impl.QueryExecutorProperties;
import org.carbondata.query.carbon.model.QueryDimension;
import org.carbondata.query.carbon.model.QueryMeasure;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.BatchRawResult;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.util.DataTypeUtil;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * Created by root1 on 28/4/16.
 */
public class RawQueryResultPreparatorImpl extends AbstractQueryResultPreparator<BatchRawResult> {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RawQueryResultPreparatorImpl.class.getName());

  public RawQueryResultPreparatorImpl(QueryExecutorProperties executerProperties,
      QueryModel queryModel) {
    super(executerProperties, queryModel);
  }

  @Override public BatchRawResult prepareQueryResult(Result scannedResult) {
    if ((null == scannedResult || scannedResult.size() < 1)) {
      return new BatchRawResult(new Object[0][0]);
    }
    List<QueryDimension> queryDimension = queryModel.getQueryDimension();
    int dimensionCount = queryDimension.size();
    int totalNumberOfColumn = dimensionCount + queryExecuterProperties.measureAggregators.length;
    Object[][] resultData = new Object[scannedResult.size()][totalNumberOfColumn];
    int currentRow = 0;
    long[] surrogateResult = null;
    int noDictionaryColumnIndex = 0;
    ByteArrayWrapper key = null;
    MeasureAggregator[] value = null;
    while (scannedResult.hasNext()) {
      key = scannedResult.getKey();
      value = scannedResult.getValue();
      surrogateResult = queryExecuterProperties.keyStructureInfo.getKeyGenerator()
          .getKeyArray(key.getDictionaryKey(),
              queryExecuterProperties.keyStructureInfo.getMaskedBytes());
      for (int i = 0; i < dimensionCount; i++) {
        if (!CarbonUtil
            .hasEncoding(queryDimension.get(i).getDimension().getEncoder(), Encoding.DICTIONARY)) {
          resultData[currentRow][i] = DataTypeUtil.getDataBasedOnDataType(
              new String(key.getNoDictionaryKeyByIndex(noDictionaryColumnIndex++)),
              queryDimension.get(i).getDimension().getDataType());
        } else {
          /*resultData[currentRow][i] = getKeyInBytes(key.getDictionaryKey(),
              queryExecuterProperties.keyStructureInfo.getMaskedBytes(),
              queryDimension.get(i).getQueryOrder(),
              queryExecuterProperties.keyStructureInfo.getKeyGenerator()
                  .getKeyByteOffsets(queryDimension.get(i).getKeyOrdinal()));*/
          resultData[currentRow][i] =
              (int) surrogateResult[queryDimension.get(i).getDimension().getKeyOrdinal()];
        }
      }

      for (int i = 0; i < queryExecuterProperties.measureAggregators.length; i++) {
        resultData[currentRow][dimensionCount + i] = value[i];
      }
      currentRow++;
    }
    if (resultData.length > 0) {
      resultData = encodeToRows(resultData);
    }
    return getResult(queryModel, resultData);
  }

  private byte[] getKeyInBytes(byte[] dictKey, int[] maskedBytes, int ordinal, int[] byteOffsets) {
    int len = byteOffsets[1] - byteOffsets[0] + 1;
    byte[] out = new byte[len];
    int k = byteOffsets[0];
    for (int i = 0; i < len; i++) {
      out[i] = dictKey[maskedBytes[k++]];
    }
    return out;
  }

  private BatchRawResult getResult(QueryModel queryModel, Object[][] convertedResult) {

    List<QueryDimension> queryDimensions = queryModel.getQueryDimension();
    int dimensionCount = queryDimensions.size();
    int msrCount = queryExecuterProperties.measureAggregators.length;
    Object[][] resultDataA = new Object[dimensionCount + msrCount][convertedResult[0].length];

    QueryDimension queryDimension = null;
    for (int columnIndex = 0; columnIndex < resultDataA[0].length; columnIndex++) {
      for (int i = 0; i < dimensionCount; i++) {
        queryDimension = queryDimensions.get(i);
        resultDataA[queryDimension.getQueryOrder()][columnIndex] = convertedResult[i][columnIndex];
      }
      MeasureAggregator[] msrAgg =
          new MeasureAggregator[queryExecuterProperties.measureAggregators.length];

      fillMeasureValueForAggGroupByQuery(queryModel, convertedResult, dimensionCount, columnIndex,
          msrAgg);

      QueryMeasure msr = null;
      for (int i = 0; i < queryModel.getQueryMeasures().size(); i++) {
        msr = queryModel.getQueryMeasures().get(i);
        if (msrAgg[queryExecuterProperties.measureStartIndex + i].isFirstTime() && (
            msr.getAggregateFunction().equals(CarbonCommonConstants.COUNT) || msr
                .getAggregateFunction().equals(CarbonCommonConstants.DISTINCT_COUNT))) {
          resultDataA[msr.getQueryOrder()][columnIndex] = 0.0;
        } else if (msrAgg[queryExecuterProperties.measureStartIndex + i].isFirstTime()) {
          resultDataA[msr.getQueryOrder()][columnIndex] = null;
        } else {
          Object msrVal;
          switch (msr.getMeasure().getDataType()) {
            case LONG:
              msrVal = msrAgg[queryExecuterProperties.measureStartIndex + i].getLongValue();
              break;
            case DECIMAL:
              msrVal = msrAgg[queryExecuterProperties.measureStartIndex + i].getBigDecimalValue();
              break;
            default:
              msrVal = msrAgg[queryExecuterProperties.measureStartIndex + i].getDoubleValue();
          }
          resultDataA[msr.getQueryOrder()][columnIndex] = DataTypeUtil
              .getMeasureDataBasedOnDataType(msrVal == null ? null : msrVal,
                  msr.getMeasure().getDataType());
        }
      }
    }
    LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG,
        "###########################################------ Total Number of records"
            + resultDataA[0].length);
    return new BatchRawResult(resultDataA);
  }
}

