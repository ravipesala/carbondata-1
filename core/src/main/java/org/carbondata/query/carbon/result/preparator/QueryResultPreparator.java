package org.carbondata.query.carbon.result.preparator;

import org.carbondata.query.carbon.result.Result;

/**
 * Created by root1 on 28/4/16.
 */
public interface QueryResultPreparator<E> {

  public E prepareQueryResult(Result scannedResult);

}
