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

package org.carbondata.query.carbon.result;

import org.carbondata.core.iterator.CarbonIterator;

/**
 * Below class holds the query result
 */
public class BatchRawResult implements CarbonIterator<Object[]> {

  /**
   * list of keys
   */
  private Object[][] rows;


  /**
   * counter to check whether all the records are processed or not
   */
  private int counter;

  private int size;

  public BatchRawResult(Object[][] rows) {
    this.rows = rows;
    if(rows.length > 0) {
      this.size = rows[0].length;
    }
  }

  public Object[][] getAllRows() {
    return rows;
  }

  /**
   * Returns {@code true} if the iteration has more elements.
   *
   * @return {@code true} if the iteration has more elements
   */
  @Override public boolean hasNext() {
    return counter < size;
  }

  /**
   * Returns the next element in the iteration.
   *
   * @return the next element in the iteration
   */
  @Override public Object[] next() {
    Object[] row = new Object[rows.length];
    for (int i = 0; i < rows.length; i++) {
      row[i] = rows[i][counter];
    }
    counter++;
    return row;
  }
}
