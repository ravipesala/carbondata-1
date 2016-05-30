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
package org.carbondata.hadoop;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.carbon.datastore.DataRefNodeFinder;
import org.carbondata.core.carbon.datastore.IndexKey;
import org.carbondata.core.carbon.datastore.SegmentTaskIndexStore;
import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.carbondata.core.carbon.datastore.exception.IndexBuilderException;
import org.carbondata.core.carbon.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.carbondata.core.carbon.datastore.impl.btree.BlockBTreeLeafNode;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.keygenerator.KeyGenException;
import org.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.carbondata.hadoop.readsupport.impl.DictionaryDecodeReadSupport;
import org.carbondata.hadoop.util.CarbonInputFormatUtil;
import org.carbondata.hadoop.util.ObjectSerializationUtil;
import org.carbondata.hadoop.util.SchemaReader;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.filter.resolver.FilterResolverIntf;
import org.carbondata.query.filters.FilterExpressionProcessor;
import org.carbondata.query.filters.measurefilter.util.FilterUtil;

import static org.carbondata.core.constants.CarbonCommonConstants.INVALID_SEGMENT_ID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.util.StringUtils;

/**
 * Carbon Input format class representing one carbon table
 */
public class CarbonInputFormat<T> extends FileInputFormat<Void, T> {

  private static final Log LOG = LogFactory.getLog(CarbonInputFormat.class);

  private static final String DATABASE_NAME = "mapreduce.input.carboninputformat.databasename";
  private static final String TABLE_NAME = "mapreduce.input.carboninputformat.tablename";
  private static final String FILTER_PREDICATE =
      "mapreduce.input.carboninputformat.filter.predicate";
  private static final String COLUMN_PROJECTION = "mapreduce.input.carboninputformat.projection";
  private static final String CARBON_TABLE = "mapreduce.input.carboninputformat.table";
  private static final String CARBON_READ_SUPPORT = "mapreduce.input.carboninputformat.readsupport";
  //comma separated list of input segment numbers
  public static final String INPUT_SEGMENT_NUMBERS =
      "mapreduce.input.carboninputformat.segmentnumbers";

  public static void setTableToAccess(Job job, CarbonTableIdentifier tableIdentifier) {
    job.getConfiguration().set(CarbonInputFormat.DATABASE_NAME, tableIdentifier.getDatabaseName());
    job.getConfiguration().set(CarbonInputFormat.TABLE_NAME, tableIdentifier.getTableName());
  }

  /**
   * Set List of segments to access
   */
  public static void setSegmentsToAccess(Job job, List<Integer> segmentNosList) {

    //serialize to comma separated string
    StringBuilder stringSegmentsBuilder = new StringBuilder();
    for (int i = 0; i < segmentNosList.size(); i++) {
      Integer segmentNo = segmentNosList.get(i);
      stringSegmentsBuilder.append(segmentNo);
      if (i < segmentNosList.size() - 1) {
        stringSegmentsBuilder.append(",");
      }
    }
    job.getConfiguration()
        .set(CarbonInputFormat.INPUT_SEGMENT_NUMBERS, stringSegmentsBuilder.toString());
  }

  /**
   * Get CarbonTableIdentifier from job configuration
   */
  public static CarbonTableIdentifier getTableToAccess(Configuration configuration) {
    String databaseName = configuration.get(CarbonInputFormat.DATABASE_NAME);
    String tableName = configuration.get(CarbonInputFormat.TABLE_NAME);
    if (databaseName != null && tableName != null) {
      return new CarbonTableIdentifier(databaseName, tableName);
    }
    //TODO: better raise exception
    return null;
  }

  /**
   * {@inheritDoc}
   * Configurations FileInputFormat.INPUT_DIR
   * are used to get table path to read.
   *
   * @param job
   * @return List<InputSplit> list of CarbonInputSplit
   * @throws IOException
   */
  @Override public List<InputSplit> getSplits(JobContext job) throws IOException {
    CarbonTable carbonTable = new SchemaReader()
        .readCarbonTableFromStore(getTablePath(job.getConfiguration()),
            getTableToAccess(job.getConfiguration()));
    setCarbonTable(job, carbonTable);
    Expression filterPredicates = getFilterPredicates(job.getConfiguration());
    if (filterPredicates == null) {
      List<InputSplit> splits = super.getSplits(job);
      List<InputSplit> carbonSplits = new ArrayList<InputSplit>(splits.size());
      // identify table blocks
      for (InputSplit inputSplit : splits) {
        FileSplit fileSplit = (FileSplit) inputSplit;
        int segmentId = CarbonTablePath.DataPathUtil.getSegmentId(fileSplit.getPath().toString());
        if (INVALID_SEGMENT_ID == segmentId) {
          continue;
        }
        carbonSplits.add(CarbonInputSplit.from(segmentId, fileSplit));
      }
      return carbonSplits;
    } else {
      try {
        CarbonInputFormatUtil.processFilterExpression(filterPredicates,
            carbonTable.getDimensionByTableName(carbonTable.getFactTableName()),
            carbonTable.getMeasureByTableName(carbonTable.getFactTableName()));
        return getSplits(job, getResolvedFilter(job.getConfiguration(), filterPredicates));
      } catch (Exception ex) {
        throw new IOException(ex.getMessage());
      }
    }
  }

  private void setCarbonTable(JobContext job, CarbonTable carbonTable) throws IOException {
    job.getConfiguration()
        .set(CARBON_TABLE, ObjectSerializationUtil.convertObjectToString(carbonTable));
  }

  private CarbonTable getCarbonTable(Configuration configuration) throws IOException {
    String carbonTableStr = configuration.get(CARBON_TABLE);
    CarbonTable carbonTable =
        (CarbonTable) ObjectSerializationUtil.convertStringToObject(carbonTableStr);
    return carbonTable;
  }

  /**
   * {@inheritDoc}
   * Configurations FileInputFormat.INPUT_DIR, CarbonInputFormat.INPUT_SEGMENT_NUMBERS
   * are used to get table path to read.
   *
   * @return
   * @throws IOException
   */
  public List<InputSplit> getSplits(JobContext job, FilterResolverIntf filterResolver)
      throws IOException, IndexBuilderException {

    List<InputSplit> result = new LinkedList<InputSplit>();

    FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();

    AbsoluteTableIdentifier absoluteTableIdentifier =
        getAbsoluteTableIdentifier(job.getConfiguration());

    //for each segment fetch blocks matching filter in Driver BTree
    for (int segmentNo : getValidSegments(job)) {
      List<DataRefNode> dataRefNodes =
          getDataBlocksOfSegment(job, filterExpressionProcessor, absoluteTableIdentifier,
              filterResolver, segmentNo);
      for (DataRefNode dataRefNode : dataRefNodes) {
        BlockBTreeLeafNode leafNode = (BlockBTreeLeafNode) dataRefNode;
        TableBlockInfo tableBlockInfo = leafNode.getTableBlockInfo();
        result.add(new CarbonInputSplit(segmentNo, new Path(tableBlockInfo.getFilePath()),
            tableBlockInfo.getBlockOffset(), tableBlockInfo.getBlockLength(),
            tableBlockInfo.getLocations()));
      }
    }
    return result;
  }

  /**
   * get total number of rows. Same as count(*)
   *
   * @throws IOException
   * @throws IndexBuilderException
   */
  public long getRowCount(JobContext job) throws IOException, IndexBuilderException {

    long rowCount = 0;
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier(
            getStorePathString(job.getConfiguration()), getTableToAccess(job.getConfiguration()));

    //for each segment fetch blocks matching filter in Driver BTree
    for (int segmentNo : getValidSegments(job)) {
      Map<String, AbstractIndex> segmentIndexMap =
          getSegmentAbstractIndexs(job, absoluteTableIdentifier, segmentNo);

      // sum number of rows of each task
      for (AbstractIndex abstractIndex : segmentIndexMap.values()) {
        rowCount += abstractIndex.getTotalNumberOfRows();
      }
    }
    return rowCount;
  }

  /**
   * {@inheritDoc}
   * Configurations FileInputFormat.INPUT_DIR, CarbonInputFormat.INPUT_SEGMENT_NUMBERS
   * are used to get table path to read.
   *
   * @return
   * @throws IOException
   */
  public FilterResolverIntf getResolvedFilter(Configuration configuration,
      Expression filterExpression)
      throws IOException, IndexBuilderException, QueryExecutionException {

    FilterExpressionProcessor filterExpressionProcessor = new FilterExpressionProcessor();
    AbsoluteTableIdentifier absoluteTableIdentifier = getAbsoluteTableIdentifier(configuration);
    //get resolved filter
    return filterExpressionProcessor.getFilterResolver(filterExpression, absoluteTableIdentifier);
  }

  private AbsoluteTableIdentifier getAbsoluteTableIdentifier(Configuration configuration)
      throws IOException {
    return new AbsoluteTableIdentifier(getStorePathString(configuration),
        getTableToAccess(configuration));
  }

  public static void setFilterPredicates(JobContext job, Expression filterExpression) {

    try {
      String filterString = ObjectSerializationUtil.convertObjectToString(filterExpression);
      job.getConfiguration().set(FILTER_PREDICATE, filterString);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Expression getFilterPredicates(Configuration configuration) {

    try {
      String filterExprString = configuration.get(FILTER_PREDICATE);
      if (filterExprString == null) {
        return null;
      }
      Expression filterExprs =
          (Expression) ObjectSerializationUtil.convertStringToObject(filterExprString);
      return filterExprs;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void setColumnProjection(CarbonProjection projection, Configuration configuration) {
    if (projection == null || projection.isEmpty()) {
      return;
    }
    String[] allColumns = projection.getAllColumns();
    StringBuilder builder = new StringBuilder();
    for (String column : allColumns) {
      builder.append(column).append(",");
    }
    String columnString = builder.toString();
    columnString = columnString.substring(0, columnString.length() - 1);
    configuration.set(COLUMN_PROJECTION, columnString);
  }

  public static void setCarbonReadSupport(Class<? extends CarbonReadSupport> readSupportClass,
      Configuration configuration) {
    if (readSupportClass != null) {
      configuration.set(CARBON_READ_SUPPORT, readSupportClass.getName());
    }
  }

  /**
   * get data blocks of given segment
   */
  private List<DataRefNode> getDataBlocksOfSegment(JobContext job,
      FilterExpressionProcessor filterExpressionProcessor,
      AbsoluteTableIdentifier absoluteTableIdentifier, FilterResolverIntf resolver, int segmentId)
      throws IndexBuilderException, IOException {

    Map<String, AbstractIndex> segmentIndexMap =
        getSegmentAbstractIndexs(job, absoluteTableIdentifier, segmentId);

    List<DataRefNode> resultFilterredBlocks = new LinkedList<DataRefNode>();

    // build result
    for (AbstractIndex abstractIndex : segmentIndexMap.values()) {

      List<DataRefNode> filterredBlocks = null;
      // if no filter is given get all blocks from Btree Index
      if (null == resolver) {
        filterredBlocks = getDataBlocksOfIndex(abstractIndex);
      } else {
        // apply filter and get matching blocks
        try {
          filterredBlocks = filterExpressionProcessor
              .getFilterredBlocks(abstractIndex.getDataRefNode(), resolver, abstractIndex,
                  absoluteTableIdentifier);
        } catch (QueryExecutionException e) {
          throw new IndexBuilderException(e.getMessage());
        }
      }
      resultFilterredBlocks.addAll(filterredBlocks);
    }
    return resultFilterredBlocks;
  }

  private Map<String, AbstractIndex> getSegmentAbstractIndexs(JobContext job,
      AbsoluteTableIdentifier absoluteTableIdentifier, int segmentId)
      throws IOException, IndexBuilderException {
    Map<String, AbstractIndex> segmentIndexMap = SegmentTaskIndexStore.getInstance()
        .getSegmentBTreeIfExists(absoluteTableIdentifier, segmentId);

    // if segment tree is not loaded, load the segment tree
    if (segmentIndexMap == null) {
      // List<FileStatus> fileStatusList = new LinkedList<FileStatus>();
      List<TableBlockInfo> tableBlockInfoList = new LinkedList<TableBlockInfo>();
      // getFileStatusOfSegments(job, new int[]{ segmentId }, fileStatusList);

      // get file location of all files of given segment
      JobContext newJob =
          new JobContextImpl(new Configuration(job.getConfiguration()), job.getJobID());
      newJob.getConfiguration().set(CarbonInputFormat.INPUT_SEGMENT_NUMBERS, segmentId + "");

      // identify table blocks
      for (InputSplit inputSplit : getSplits(newJob)) {
        CarbonInputSplit carbonInputSplit = (CarbonInputSplit) inputSplit;
        tableBlockInfoList.add(
            new TableBlockInfo(carbonInputSplit.getPath().toString(), carbonInputSplit.getStart(),
                segmentId, carbonInputSplit.getLocations(), carbonInputSplit.getLength()));
      }

      Map<Integer, List<TableBlockInfo>> segmentToTableBlocksInfos = new HashMap<>();
      segmentToTableBlocksInfos.put(segmentId, tableBlockInfoList);

      // get Btree blocks for given segment
      segmentIndexMap = SegmentTaskIndexStore.getInstance()
          .loadAndGetTaskIdToSegmentsMap(segmentToTableBlocksInfos, absoluteTableIdentifier);

    }
    return segmentIndexMap;
  }

  /**
   * get data blocks of given btree
   */
  private List<DataRefNode> getDataBlocksOfIndex(AbstractIndex abstractIndex) {
    List<DataRefNode> blocks = new LinkedList<DataRefNode>();
    SegmentProperties segmentProperties = abstractIndex.getSegmentProperties();

    try {
      IndexKey startIndexKey = FilterUtil.prepareDefaultStartIndexKey(segmentProperties);
      IndexKey endIndexKey = FilterUtil.prepareDefaultEndIndexKey(segmentProperties);

      // Add all blocks of btree into result
      DataRefNodeFinder blockFinder =
          new BTreeDataRefNodeFinder(segmentProperties.getDimensionColumnsValueSize());
      DataRefNode startBlock =
          blockFinder.findFirstDataBlock(abstractIndex.getDataRefNode(), startIndexKey);
      DataRefNode endBlock =
          blockFinder.findLastDataBlock(abstractIndex.getDataRefNode(), endIndexKey);
      while (startBlock != endBlock) {
        blocks.add(startBlock);
        startBlock = startBlock.getNextDataRefNode();
      }
      blocks.add(endBlock);

    } catch (KeyGenException e) {
      LOG.error("Could not generate start key", e);
    }
    return blocks;
  }

  @Override public RecordReader<Void, T> createRecordReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    CarbonTable carbonTable = getCarbonTable(taskAttemptContext.getConfiguration());
    Expression filterPredicates = getFilterPredicates(taskAttemptContext.getConfiguration());
    String columnString = taskAttemptContext.getConfiguration().get(COLUMN_PROJECTION);
    String[] columns = null;
    if (columnString != null) {
      columns = columnString.split(",");
    }
    QueryModel queryModel = CarbonInputFormatUtil
        .createQueryModel(getAbsoluteTableIdentifier(taskAttemptContext.getConfiguration()),
            carbonTable, columns, filterPredicates);
    try {
      queryModel.setFilterExpressionResolverTree(
          getResolvedFilter(taskAttemptContext.getConfiguration(), filterPredicates));
    } catch (Exception e) {
      throw new IOException(e);
    }

    CarbonReadSupport readSupport = getReadSupportClass(taskAttemptContext);

    return new CarbonRecordReader<T>(queryModel, readSupport);
  }

  private CarbonReadSupport getReadSupportClass(TaskAttemptContext taskAttemptContext) {
    String readSupportClass = taskAttemptContext.getConfiguration().get(CARBON_READ_SUPPORT);
    //By default it uses dictionary decoder read class
    CarbonReadSupport readSupport = new DictionaryDecodeReadSupport();
    if (readSupportClass != null) {
      try {
        Class<?> myClass = Class.forName(readSupportClass);
        Constructor<?> constructor = myClass.getConstructors()[0];
        readSupport = (CarbonReadSupport) constructor.newInstance();
      } catch (ClassNotFoundException ex) {
        LOG.error("Class " + readSupportClass + "not found", ex);
      } catch (Exception ex) {
        LOG.error("Error while creating " + readSupportClass, ex);
      }
    }
    return readSupport;
  }

  @Override protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
    return super.computeSplitSize(blockSize, minSize, maxSize);
  }

  @Override protected int getBlockIndex(BlockLocation[] blkLocations, long offset) {
    return super.getBlockIndex(blkLocations, offset);
  }

  @Override protected List<FileStatus> listStatus(JobContext job) throws IOException {
    List<FileStatus> result = new ArrayList<FileStatus>();
    int[] segmentsToConsider = getValidSegments(job);
    if (segmentsToConsider.length == 0) {
      throw new IOException("No segments found");
    }

    getFileStatusOfSegments(job, segmentsToConsider, result);
    return result;
  }

  private void getFileStatusOfSegments(JobContext job, int[] segmentsToConsider,
      List<FileStatus> result) throws IOException {
    String[] partitionsToConsider = getValidPartitions(job);
    if (partitionsToConsider.length == 0) {
      throw new IOException("No partitions/data found");
    }

    PathFilter inputFilter = getDataFileFilter(job);
    CarbonTablePath tablePath = getTablePath(job.getConfiguration());

    // get tokens for all the required FileSystem for table path
    TokenCache.obtainTokensForNamenodes(job.getCredentials(), new Path[] { tablePath },
        job.getConfiguration());

    //get all data files of valid partitions and segments
    for (int i = 0; i < partitionsToConsider.length; ++i) {
      String partition = partitionsToConsider[i];

      for (int j = 0; j < segmentsToConsider.length; ++j) {
        int segmentId = segmentsToConsider[j];
        Path segmentPath = new Path(tablePath.getCarbonDataDirectoryPath(partition, segmentId));
        FileSystem fs = segmentPath.getFileSystem(job.getConfiguration());

        RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(segmentPath);
        while (iter.hasNext()) {
          LocatedFileStatus stat = iter.next();
          if (inputFilter.accept(stat.getPath())) {
            if (stat.isDirectory()) {
              addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
            } else {
              result.add(stat);
            }
          }
        }
      }
    }
  }

  public Path getStorePath(JobContext job) throws IOException {

    String storePathString = getStorePathString(job.getConfiguration());
    return new CarbonStorePath(storePathString);
  }

  public CarbonTablePath getTablePath(Configuration configuration) throws IOException {

    String storePathString = getStorePathString(configuration);
    CarbonTableIdentifier tableIdentifier = CarbonInputFormat.getTableToAccess(configuration);
    if (tableIdentifier == null) {
      throw new IOException("Could not find " + DATABASE_NAME + "," + TABLE_NAME);
    }
    return CarbonStorePath.getCarbonTablePath(storePathString, tableIdentifier);
  }

  /**
   * @param job
   * @return the PathFilter for Fact Files.
   */
  public PathFilter getDataFileFilter(JobContext job) {
    return new CarbonPathFilter(getUpdateExtension());
  }

  private static String getStorePathString(Configuration configuration) throws IOException {

    String dirs = configuration.get(INPUT_DIR, "");
    String[] inputPaths = StringUtils.split(dirs);
    if (inputPaths.length == 0) {
      throw new IOException("No input paths specified in job");
    }
    return inputPaths[0];
  }

  /**
   * required to be moved to core
   *
   * @return updateExtension
   */
  private String getUpdateExtension() {
    // TODO: required to modify when supporting update, mostly will be update timestamp
    return "update";
  }

  /**
   * @return updateExtension
   */
  private int[] getValidSegments(JobContext job) throws IOException {
    String segmentString = job.getConfiguration().get(INPUT_SEGMENT_NUMBERS, "");
    // if no segments
    if (segmentString.trim().isEmpty()) {
      return new int[0];
    }

    String[] segments = segmentString.split(",");
    int[] segmentIds = new int[segments.length];
    int i = 0;
    try {
      for (; i < segments.length; i++) {
        segmentIds[i] = Integer.parseInt(segments[i]);
      }
    } catch (NumberFormatException e) {
      throw new IOException("segment no:" + segments[i] + " should be integer");
    }
    return segmentIds;
  }

  /**
   * required to be moved to core
   *
   * @return updateExtension
   */
  private String[] getValidPartitions(JobContext job) {
    //TODO: has to Identify partitions by partition pruning
    return new String[] { "0" };
  }
}
