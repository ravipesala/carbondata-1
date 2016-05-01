package org.carbondata.hadoop.readsupport.impl;

import org.carbondata.core.cache.Cache;
import org.carbondata.core.cache.CacheProvider;
import org.carbondata.core.cache.CacheType;
import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonColumn;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.hadoop.readsupport.CarbonReadSupport;

/**
 * Created by root1 on 1/5/16.
 */
public class DictionaryDecodeReadSupport implements CarbonReadSupport<Object[]> {

  private Dictionary[] dictionaries;

  /**
   * It would be instantiated in side the task so the dictionary would be loaded inside every mapper
   * instead of driver.
   * @param carbonColumns
   * @param absoluteTableIdentifier
   */
  @Override public void intialize(CarbonColumn[] carbonColumns,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    dictionaries = new Dictionary[carbonColumns.length];
    for (int i = 0; i < carbonColumns.length; i++) {
      if (carbonColumns[i].hasEncoding(Encoding.DICTIONARY)) {
        CacheProvider cacheProvider = CacheProvider.getInstance();
        Cache<DictionaryColumnUniqueIdentifier, Dictionary> forwardDictionaryCache = cacheProvider
            .createCache(CacheType.FORWARD_DICTIONARY, absoluteTableIdentifier.getStorePath());
        try {
          dictionaries[i] = forwardDictionaryCache.get(new DictionaryColumnUniqueIdentifier(
              absoluteTableIdentifier.getCarbonTableIdentifier(), carbonColumns[i].getColumnId()));
        } catch (CarbonUtilException e) {
          //TODO : create exception class and throw
          throw new RuntimeException(e);
        }
      }

    }
  }

  @Override public Object[] readRow(Object[] data) {
    for (int i = 0; i < dictionaries.length; i++) {
      if (dictionaries[i] != null) {
        data[i] = dictionaries[i].getDictionaryValueForKey((int) data[i]);
      }
    }
    return data;
  }
}
