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
package org.carbondata.query.carbon.result.iterator;

import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.carbon.datastore.DataRefNodeFinder;
import org.carbondata.core.carbon.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.query.carbon.executor.impl.QueryExecutorProperties;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.internal.InternalQueryExecutor;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.processor.impl.DataBlockIteratorImpl;

/**
 * In case of detail query we cannot keep all the records in memory so for
 * executing that query are returning a iterator over block and every time next
 * call will come it will execute the block and return the result
 */
public abstract class AbstractDetailQueryResultIterator extends CarbonIterator {

  /**
   * LOGGER.
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractDetailQueryResultIterator.class.getName());

  /**
   * execution info of the block
   */
  protected List<BlockExecutionInfo> blockExecutionInfos;

  /**
   * executor which will execute the query
   */
  protected InternalQueryExecutor executor;

  /**
   * number of cores which can be used
   */
  private long numberOfCores;

  /**
   * file reader which will be used to execute the query
   */
  protected FileHolder fileReader;

  /**
   * block index to be executed
   */
  protected int[] blockIndexToBeExecuted;

  protected DataBlockIteratorImpl dataBlockProcessor;

  protected boolean nextBatch = false;

  public AbstractDetailQueryResultIterator(List<BlockExecutionInfo> infos,
      QueryExecutorProperties executerProperties, QueryModel queryModel) {
    int recordSize = 0;
    String defaultInMemoryRecordsSize =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.INMEMORY_REOCRD_SIZE);
    if (null != defaultInMemoryRecordsSize) {
      try {
        recordSize = Integer.parseInt(defaultInMemoryRecordsSize);
      } catch (NumberFormatException ne) {
        LOGGER.error("Invalid inmemory records size. Using default value");
        recordSize = CarbonCommonConstants.INMEMORY_REOCRD_SIZE_DEFAULT;
      }
    }
    this.numberOfCores = recordSize / Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.BLOCKLET_SIZE,
            CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL));
    if (numberOfCores == 0) {
      numberOfCores++;
    }
    this.blockExecutionInfos = infos;
    this.blockIndexToBeExecuted = new int[(int) numberOfCores];
    this.fileReader = FileFactory.getFileHolder(
        FileFactory.getFileType(queryModel.getAbsoluteTableIdentifier().getStorePath()));
    intialiseInfos();
  }

  protected void intialiseInfos() {
    for (BlockExecutionInfo blockInfo : blockExecutionInfos) {
      DataRefNodeFinder finder = new BTreeDataRefNodeFinder(blockInfo.getEachColumnValueSize());
      DataRefNode startDataBlock = finder
          .findFirstDataBlock(blockInfo.getDataBlock().getDataRefNode(), blockInfo.getStartKey());
      DataRefNode endDataBlock = finder
          .findLastDataBlock(blockInfo.getDataBlock().getDataRefNode(), blockInfo.getEndKey());
      long numberOfBlockToScan = endDataBlock.nodeNumber() - startDataBlock.nodeNumber() + 1;
      blockInfo.setFirstDataBlock(startDataBlock);
      blockInfo.setNumberOfBlockToScan(numberOfBlockToScan);
    }
  }

  @Override public boolean hasNext() {
    if ((dataBlockProcessor != null && dataBlockProcessor.hasNext()) || nextBatch) {
      return true;
    } else {
      dataBlockProcessor = getDataBlockProcessor();
      while (dataBlockProcessor != null) {
        if (dataBlockProcessor.hasNext()) {
          return true;
        }
        dataBlockProcessor = getDataBlockProcessor();
      }
      return false;
    }
  }

  private DataBlockIteratorImpl getDataBlockProcessor() {
    if(blockExecutionInfos.size() > 0) {
      BlockExecutionInfo executionInfo = blockExecutionInfos.get(0);
      blockExecutionInfos.remove(executionInfo);
      return new DataBlockIteratorImpl(executionInfo, fileReader, 5000);
    }
    return null;
  }


}
