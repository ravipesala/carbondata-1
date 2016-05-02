/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.hadoop.util;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.hadoop.exception.CarbonInputFormatException;
import org.carbondata.query.carbon.model.DimensionAggregatorInfo;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.directinterface.impl.CarbonQueryParseUtil;
import org.carbondata.query.expression.ColumnExpression;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.conditional.ConditionalExpression;
import org.carbondata.query.filter.resolver.FilterResolverIntf;
import org.carbondata.query.filters.FilterExpressionProcessor;

/**
 * Utility class
 */
public class CarbonInputFormatUtil {

  public static QueryModel createQueryModel(AbsoluteTableIdentifier absoluteTableIdentifier,
      CarbonTable carbonTable, String columnString) throws CarbonInputFormatException {
    QueryModel executorModel = new QueryModel();
    String[] columns = null;
    if (columnString != null) {
      columns = columnString.split(",");
    }

    String factTableName = carbonTable.getFactTableName();
    executorModel.setAbsoluteTableIdentifier(absoluteTableIdentifier);

    fillExecutorModel(carbonTable, executorModel, factTableName, columns);
    List<CarbonDimension> dims =
        new ArrayList<CarbonDimension>(CarbonCommonConstants.CONSTANT_SIZE_TEN);

    dims.addAll(executorModel.getQueryDimension());

    executorModel.setDimAggregationInfo(new ArrayList<DimensionAggregatorInfo>());
    executorModel.setForcedDetailRawQuery(true);
    executorModel.setQueryId(System.nanoTime() + "");
    return executorModel;
  }

  private static void fillExecutorModel(CarbonTable carbonTable, QueryModel queryModel,
      String factTableName, String[] columns) throws CarbonInputFormatException {

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
        if (dimensionByName != null) {
          dimensionByName.setQueryOrder(i++);
          carbonDimensions.add(dimensionByName);
        } else {
          CarbonMeasure measure = carbonTable.getMeasureByName(factTableName, column);
          if (measure == null) {
            throw new CarbonInputFormatException(
                column + " column not found in the table " + factTableName);
          }
          measure.setQueryOrder(i++);
          carbonMsrs.add(measure);
        }
      }
    }

    queryModel.setQueryDimension(carbonDimensions);
    queryModel.setQueryMeasures(carbonMsrs);
    queryModel.setSortOrder(new byte[0]);
    queryModel.setSortDimension(new ArrayList<CarbonDimension>(0));
    queryModel.setLimit(-1);

    // fill measures
    List<CarbonMeasure> carbonMeasures = carbonTable.getMeasureByTableName(factTableName);

  }

  public static void processFilterExpression(Expression filterExpression, CarbonTable carbonTable) {
    List<CarbonDimension> dimensions =
        carbonTable.getDimensionByTableName(carbonTable.getFactTableName());
    List<CarbonMeasure> measures =
        carbonTable.getMeasureByTableName(carbonTable.getFactTableName());
    processFilterExpression(filterExpression, dimensions, measures);
  }

  private static void processFilterExpression(Expression filterExpression,
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

  /**
   * It gets the projection columns
   */
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

  /**
   * Resolve the filter expression.
   * @param filterExpression
   * @param absoluteTableIdentifier
   * @return
   * @throws CarbonInputFormatException
   */
  public static FilterResolverIntf resolveFilter(Expression filterExpression,
      AbsoluteTableIdentifier absoluteTableIdentifier)
      throws CarbonInputFormatException {
    try {
      FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();
      //get resolved filter
      return filterExpressionProcessor.getFilterResolver(filterExpression, absoluteTableIdentifier);
    } catch (Exception e) {
      throw new CarbonInputFormatException("Error while resolving filter expression", e);
    }
  }

  public static String processPath(String path) {
    if (path != null && path.startsWith("file:")) {
      return path.substring(5, path.length());
    }
    return path;
  }
}
