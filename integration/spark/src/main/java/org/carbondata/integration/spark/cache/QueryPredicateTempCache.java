package org.carbondata.integration.spark.cache;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.map.HashedMap;

/**
 * Created by root1 on 30/4/16.
 */
public final class QueryPredicateTempCache {

  // TODO : Make LRU cache
  private Map<String, String> cache = new HashedMap();

  private AtomicInteger atomicInteger = new AtomicInteger(0);

  private QueryPredicateTempCache(){
  }

  public static QueryPredicateTempCache instance = new QueryPredicateTempCache();

  public String getSurrogate(String predicate) {
    synchronized (this) {
      String value = cache.get(predicate);
      if (value == null) {
        value = atomicInteger.incrementAndGet() + "";
        cache.put(predicate, value);
      }
      return value;
    }
  }

  public String getPredicate(String surrogate) {
    for(Map.Entry<String, String> entry : cache.entrySet()) {
      if(entry.getValue().equals(surrogate)) {
        return entry.getKey();
      }
    }
    return null;
  }

}
