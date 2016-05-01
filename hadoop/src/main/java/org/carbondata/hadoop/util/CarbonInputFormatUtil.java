package org.carbondata.hadoop.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.carbon.model.DimensionAggregatorInfo;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.directinterface.impl.CarbonQueryParseUtil;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.conditional.ConditionalExpression;

/**
 * Created by root1 on 30/4/16.
 */
public class CarbonInputFormatUtil {

  public static QueryModel createQueryModel(AbsoluteTableIdentifier absoluteTableIdentifier,
      CarbonTable carbonTable, String[] columns, Expression filterExpression) throws IOException {
    QueryModel executorModel = new QueryModel();
    //TODO : Need to find out right table as per the dims and msrs requested.

    String factTableName = carbonTable.getFactTableName();
    executorModel.setAbsoluteTableIdentifier(absoluteTableIdentifier);

    fillExecutorModel(carbonTable, executorModel, factTableName, columns, filterExpression);
    List<CarbonDimension> dims =
        new ArrayList<CarbonDimension>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

    dims.addAll(executorModel.getQueryDimension());

    executorModel.setDimAggregationInfo(new ArrayList<DimensionAggregatorInfo>());
    executorModel.setForcedDetailRawQuery(true);
    executorModel.setQueryId(System.nanoTime() + "");
    return executorModel;
  }

  private static void fillExecutorModel(CarbonTable carbonTable, QueryModel queryModel,
      String factTableName, String[] columns, Expression filterExpression) {

    // fill dimensions
    List<CarbonDimension> carbonDimensions = new ArrayList<CarbonDimension>();
    List<CarbonMeasure> carbonMsrs = new ArrayList<CarbonMeasure>();

    int i = 0;
    // If columns are null, set all dimensions and measures
    if (columns == null) {
      carbonMsrs.addAll(carbonTable.getMeasureByTableName(factTableName));
      carbonDimensions.addAll(carbonTable.getDimensionByTableName(factTableName));
      for (CarbonDimension dimension : carbonDimensions) {
        dimension.setQueryOrder(i++);
      }
      for (CarbonMeasure measure : carbonMsrs) {
        measure.setQueryOrder(i++);
      }
    } else {
      for (String column : columns) {
        CarbonDimension dimensionByName = carbonTable.getDimensionByName(factTableName, column);
        dimensionByName.setQueryOrder(i++);
        if (dimensionByName != null) {
          carbonDimensions.add(dimensionByName);
        } else {
          CarbonMeasure measure = carbonTable.getMeasureByName(factTableName, column);
          measure.setQueryOrder(i++);
          carbonMsrs.add(measure);
        }
      }
    }

    queryModel.setQueryDimension(carbonDimensions);
    queryModel.setQueryMeasures(carbonMsrs);
    queryModel.setSortOrder(new byte[0]);
    queryModel.setSortDimension(new ArrayList<CarbonDimension>(0));

    // fill measures
    List<CarbonMeasure> carbonMeasures = carbonTable.getMeasureByTableName(factTableName);

    // fill filter Column Expression
    if (null != filterExpression) {
      processFilterExpression(filterExpression, carbonDimensions, carbonMeasures);
    }

  }

  public static void processFilterExpression(Expression filterExpression,
      List<CarbonDimension> dimensions, List<CarbonMeasure> measures) {
    if (null != filterExpression) {
      if (null != filterExpression.getChildren() && filterExpression.getChildren().size() == 0) {
        if (filterExpression instanceof ConditionalExpression) {
          List<ColumnExpression> listOfCol =
              ((ConditionalExpression) filterExpression).getColumnList();
          for (ColumnExpression expression : listOfCol) {
            setDimAndMsrColumnNode(dimensions, measures, (ColumnExpression) expression);
          }

        }
      }
      for (Expression expression : filterExpression.getChildren()) {

        if (expression instanceof ColumnExpression) {
          setDimAndMsrColumnNode(dimensions, measures, (ColumnExpression) expression);
        } else {
          processFilterExpression(expression, dimensions, measures);
        }
      }
    }

  }

  private static void setDimAndMsrColumnNode(List<CarbonDimension> dimensions,
      List<CarbonMeasure> measures, ColumnExpression col) {
    CarbonDimension dim;
    CarbonMeasure msr;
    String columnName;
    columnName = col.getColumnName();
    dim = CarbonQueryParseUtil.findDimension(dimensions, columnName);
    col.setCarbonColumn(dim);
    col.setDimension(dim);
    col.setDimension(true);
    if (null == dim) {
      msr = getCarbonMetadataMeasure(columnName, measures);
      col.setCarbonColumn(msr);
      col.setDimension(false);
    }
  }

  private static CarbonMeasure getCarbonMetadataMeasure(String name, List<CarbonMeasure> measures) {
    for (CarbonMeasure measure : measures) {
      if (measure.getColName().equalsIgnoreCase(name)) {
        return measure;
      }
    }
    return null;
  }

  public static CarbonColumn[] getProjectionColumns(QueryModel queryModel) {
    CarbonColumn[] carbonColumns =
        new CarbonColumn[queryModel.getQueryDimension().size() + queryModel.getQueryMeasures()
            .size()];
    for (CarbonDimension dimension : queryModel.getQueryDimension()) {
      carbonColumns[dimension.getQueryOrder()] = dimension;
    }
    for (CarbonMeasure msr : queryModel.getQueryMeasures()) {
      carbonColumns[msr.getQueryOrder()] = msr;
    }
    return carbonColumns;
  }
}
