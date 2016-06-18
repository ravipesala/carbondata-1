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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.impl.QueryExecutorProperties;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.internal.InternalQueryExecutor;
import org.carbondata.query.carbon.model.QueryModel;
import org.carbondata.query.carbon.result.BatchResult;
import org.carbondata.query.carbon.result.ListBasedResultWrapper;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.result.preparator.QueryResultPreparator;
import org.carbondata.query.carbon.result.preparator.impl.RawQueryResultPreparatorImpl;

/**
 * In case of detail query we cannot keep all the records in memory so for
 * executing that query are returning a iterator over block and every time next
 * call will come it will execute the block and return the result
 */
public class DetailRawQueryResultIterator extends AbstractDetailQueryResultIterator {

  private ExecutorService execService = Executors.newFixedThreadPool(1);

  private Future<ResultInfo> future;

  private QueryResultPreparator<List<ListBasedResultWrapper>, Object> queryResultPreparator;

  public DetailRawQueryResultIterator(List<BlockExecutionInfo> infos,
      QueryExecutorProperties executerProperties, QueryModel queryModel,
      InternalQueryExecutor queryExecutor) {
    super(infos, executerProperties, queryModel, queryExecutor);
    this.queryResultPreparator = new RawQueryResultPreparatorImpl(executerProperties, queryModel);
  }

  @Override public BatchResult next() {
    BatchResult result;
    try {
      if (future == null) {
        future = execute();
      }
      ResultInfo resultFromFuture = getResultFromFuture(future);
      result = resultFromFuture.result;
      currentCounter += resultFromFuture.counter;
      if (hasNext()) {
        future = execute();
      } else {
        fileReader.finish();
      }
    } catch (QueryExecutionException e) {
      fileReader.finish();
      throw new RuntimeException(e.getCause().getMessage());
    }
    return result;
  }

  private ResultInfo getResultFromFuture(Future<ResultInfo> future) throws QueryExecutionException {
    try {
      return future.get();
    } catch (Exception e) {
      throw new QueryExecutionException(e.getMessage());
    }
  }

  private Future<ResultInfo> execute() {
    return execService.submit(new Callable<ResultInfo>() {
      @Override public ResultInfo call() throws QueryExecutionException {
        int counter = updateSliceIndexToBeExecuted();
        CarbonIterator<Result> result =
            executor.executeQuery(blockExecutionInfos, blockIndexToBeExecuted, fileReader);
        for (int i = 0; i < blockIndexToBeExecuted.length; i++) {
          if (blockIndexToBeExecuted[i] != -1) {
            blockExecutionInfos.get(blockIndexToBeExecuted[i]).setFirstDataBlock(
                blockExecutionInfos.get(blockIndexToBeExecuted[i]).getFirstDataBlock()
                    .getNextDataRefNode());
          }
        }
        BatchResult batchResult;
        if (null != result) {
          Result next = result.next();
          batchResult = queryResultPreparator.prepareQueryResult(next);
        } else {
          batchResult = queryResultPreparator.prepareQueryResult(null);
        }
        ResultInfo resultInfo = new ResultInfo();
        resultInfo.counter = counter;
        resultInfo.result = batchResult;
        return resultInfo;
      }
    });
  }

  private static class ResultInfo {
    private int counter;
    private BatchResult result;
  }
}
