/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.search.function;

import java.io.IOException;
import java.io.DataInputStream;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.File;
import java.io.EOFException;
import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.solr.common.util.ExecutorUtil;
import org.apache.solr.common.util.SolrNamedThreadFactory;
import org.apache.solr.handler.RequestHandlerBase;
import org.apache.solr.handler.RequestHandlerUtils;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.security.AuthorizationContext;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.util.external.ExternalFileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Obtains float field values from an external file.
 *
 * @see org.apache.solr.schema.ExternalFileField
 * @see org.apache.solr.schema.ExternalFileFieldReloader
 */
public class FileFloatSource2 extends ValueSource {

  private SchemaField field;
  private final SchemaField keyField;
  private final float defVal;
  private final String dataDir;
  private final File externalDir;
  private final String fileNameStripped;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Creates a new FileFloatSource
   *
   * @param field the source's SchemaField
   * @param keyField the field to use as a key
   * @param defVal the default value to use if a field has no entry in the external file
   * @param dataDir the directory in which to look for the external file
   */
  public FileFloatSource2(SchemaField field, SchemaField keyField, float defVal, String dataDir, String externalDir) {
    this.field = field;
    this.fileNameStripped = field.getName().replace("_ef$", "");
    this.keyField = keyField;
    this.defVal = defVal;
    this.dataDir = dataDir;
    this.externalDir = new File(externalDir);
  }

  @Override
  public String description() {
    return "float(" + field + ')';
  }

  @Override
  public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
      throws IOException {
    final int off = readerContext.docBase;
    IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(readerContext);

    final float[] arr = getCachedFloats(topLevelContext.reader());
    return new FloatDocValues(this) {
      @Override
      public float floatVal(int doc) {
        return arr[doc + off];
      }

      @Override
      public Object objectVal(int doc) {
        return floatVal(doc); // TODO: keep track of missing values
      }
    };
  }

  @Override
  public boolean equals(Object o) {
    if (o.getClass() != FileFloatSource2.class) return false;
    FileFloatSource2 other = (FileFloatSource2) o;
    return this.field.getName().equals(other.field.getName())
        && this.keyField.getName().equals(other.keyField.getName())
        && this.defVal == other.defVal
        && this.dataDir.equals(other.dataDir);
  }

  @Override
  public int hashCode() {
    return FileFloatSource2.class.hashCode() + field.getName().hashCode();
  }
  ;

  @Override
  public String toString() {
    return "FileFloatSource(field="
        + field.getName()
        + ",keyField="
        + keyField.getName()
        + ",defVal="
        + defVal
        + ",dataDir="
        + dataDir
        + ")";
  }

  /** Remove all cached entries. Values are lazily loaded next time getValues() is called. */
  public static void resetCache() {
    floatCache.resetCache();
  }

  /**
   * Refresh the cache for an IndexReader. The new values are loaded in the background and then
   * swapped in, so queries against the cache should not block while the reload is happening.
   *
   * @param reader the IndexReader whose cache needs refreshing
   */
  public void refreshCache(IndexReader reader) {
    if (log.isInfoEnabled()) {
      log.info("Refreshing FileFloatSource cache for field {}", this.field.getName());
    }
    floatCache.refresh(reader, new Entry(this));
    if (log.isInfoEnabled()) {
      log.info("FileFloatSource cache for field {} reloaded", this.field.getName());
    }
  }

  private final float[] getCachedFloats(IndexReader reader) {
    return (float[]) floatCache.get(reader, new Entry(this));
  }

  static Cache floatCache =
      new Cache() {
        @Override
        protected Object createValue(IndexReader reader, Object key, CachedFloats cachedFloats) {
          return getFloats(((Entry) key).ffs, reader, cachedFloats);
        }
      };

  /** Internal cache. (from lucene FieldCache) */
  abstract static class Cache {
    private final Map<IndexReader, Map<Object, Object>> readerCache = new WeakHashMap<>();

    protected abstract Object createValue(IndexReader reader, Object key, CachedFloats cachedFloats);

    public void refresh(IndexReader reader, Object key) {
      Object refreshedValues = createValue(reader, key, null);
      synchronized (readerCache) {
        Map<Object, Object> innerCache = readerCache.computeIfAbsent(reader, k -> new HashMap<>());
        innerCache.put(key, refreshedValues);
      }
    }

    public Object get(IndexReader reader, Object key) {
      Map<Object, Object> innerCache;
      Object value;
      long timeStamp = System.currentTimeMillis();
      CachedFloats cachedFloats = null;
      synchronized (readerCache) {
        innerCache = readerCache.get(reader);
        if (innerCache == null) {
          innerCache = new LRUCache<>(30);
          readerCache.put(reader, innerCache);
          value = null;
        } else {
          value = innerCache.get(key);
        }

        if (value == null) {
          value = new CreationPlaceholder();
          innerCache.put(key, value);
        } else {
          cachedFloats = (CachedFloats) value;
          if (timeStamp - cachedFloats.lastChecked > cachedFloats.ttl) {
            value = new CreationPlaceholder();
            innerCache.put(key, value);
          }
        }
      }

      if (value instanceof CreationPlaceholder) {
        synchronized (value) {
          CreationPlaceholder progress = (CreationPlaceholder) value;
          if (progress.value == null) {
            progress.value = createValue(reader, key, cachedFloats);
            if(progress.value == null) {
              // Floats were not loaded
              if(cachedFloats != null) {
                cachedFloats.lastChecked = timeStamp;
                return cachedFloats.floats;
              }
            } else {
              // Floats were loaded
              CachedFloats _cachedFloats = new CachedFloats();
              _cachedFloats.floats = (float[]) progress.value;
              _cachedFloats.lastChecked = _cachedFloats.loadTime = timeStamp;
              synchronized (readerCache) {
                innerCache.put(key, _cachedFloats);
                onlyForTesting = progress.value;
              }
              return progress.value;
            }
          }
          return progress.value;
        }
      }

      return ((CachedFloats)value).floats;
    }

    public void resetCache() {
      synchronized (readerCache) {
        // Map.clear() is optional and can throw UnsupportedOperationException,
        // but readerCache is WeakHashMap and it supports clear().
        readerCache.clear();
      }
    }
  }

  static Object onlyForTesting; // set to the last value

  static final class CreationPlaceholder {
    Object value;
  }

  /** Expert: Every composite-key in the internal cache is of this type. */
  private static class Entry {
    final FileFloatSource2 ffs;

    public Entry(FileFloatSource2 ffs) {
      this.ffs = ffs;
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Entry)) return false;
      Entry other = (Entry) o;
      return ffs.equals(other.ffs);
    }

    @Override
    public int hashCode() {
      return ffs.hashCode();
    }
  }

  private static float[] getFloats(FileFloatSource2 ffs, IndexReader reader, CachedFloats cachedFloats) {

    File latestExternalDir = getLatestFileDir(ffs.externalDir, ffs.fileNameStripped, cachedFloats == null ? -1 : cachedFloats.loadTime);

    ExecutorService executorService = ExecutorUtil.newMDCAwareCachedThreadPool(8, new SolrNamedThreadFactory("FileFloatSource2"));
    float[] vals = new float[reader.maxDoc()];
    if (ffs.defVal != 0) {
      Arrays.fill(vals, ffs.defVal);
    }

    if(latestExternalDir == null) {
      return vals;
    }

    List<Future> futures = new ArrayList();
    try {

      for(int i=0; i<8; i++) {
        MergeJoin merger = new MergeJoin(new File(ffs.dataDir, "index_"+i+".bin"), new File(latestExternalDir, "external_"+i+".bin"), vals);
        Future future = executorService.submit(merger);
        futures.add(future);
      }

      for(Future future : futures) {
        future.get();
      }

    } catch (Exception e) {
      log.error("Error loading floats", e);
    } finally {
      executorService.shutdown();;
    }

    return vals;
  }

  public static class MergeJoin implements Runnable {

    private final File indexFile;
    private final File externalFile;
    private final float[] vals;

    public MergeJoin(File indexFile, File externalFile, float[] vals) {
      this.indexFile = indexFile;
      this.externalFile = externalFile;
      this.vals = vals;
    }

    public void run() {
      DataInputStream indexIn = null;
      DataInputStream externalIn = null;
      byte[] indexIdBytes = new byte[30];
      byte[] externalIdBytes = new byte[30];

      try {
        indexIn = new DataInputStream(new BufferedInputStream(new FileInputStream(indexFile), 16384));
        externalIn = new DataInputStream(new BufferedInputStream(new FileInputStream(externalFile), 16384));
        int indexIdLength = indexIn.readByte();
        int luceneId = indexIn.readInt();
        indexIn.read(indexIdBytes, 0, indexIdLength);

        int externalIdLength = externalIn.readByte();
        float price = externalIn.readFloat();
        externalIn.read(externalIdBytes, 0, externalIdLength);

        while(true) {
          int value = ExternalFileUtil.compare(indexIdBytes, indexIdLength, externalIdBytes, externalIdLength);
          if(value == 0) {
            vals[luceneId] = price;
            indexIdLength = indexIn.readByte();
            luceneId = indexIn.readInt();
            indexIn.read(indexIdBytes, 0, indexIdLength);
            externalIdLength = externalIn.readByte();
            price = externalIn.readFloat();
            externalIn.read(externalIdBytes, 0, externalIdLength);
          } else if (value < 1) {
            // Advance only the index
            indexIdLength = indexIn.readByte();
            luceneId = indexIn.readInt();
            indexIn.read(indexIdBytes, 0, indexIdLength);
          } else {
            // Advance only the external
            externalIdLength = externalIn.readByte();
            price = externalIn.readFloat();
            externalIn.read(externalIdBytes, 0, externalIdLength);
          }
        }
      } catch (EOFException f) {
        //Do nothing
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        try {
          indexIn.close();
        } catch(Exception e) {

        }
      }

      try {
        externalIn.close();
      } catch(Exception e) {

      }
    }
  }

  public static class LRUCache<K, V> extends LinkedHashMap<K, V> {
    private final int maxSize;

    public LRUCache(int maxSize) {
      super(maxSize*2, .75f, true);
      this.maxSize = maxSize;
    }

    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
      return size() > this.maxSize;
    }
  }

  public static class CachedFloats {
    long loadTime;
    long lastChecked;
    long ttl = 300000;
    float[] floats;
  }

  public static File getLatestFileDir(File root, String fileName, long afterTime) {

    File fileHome = new File(new File(root, ExternalFileUtil.getHashDir(fileName)), fileName);
    if(!fileHome.exists()) {
      return null;
    } else {
      File[] files = fileHome.listFiles();
      if(files.length == 0) {
        return null;
      } else {
        long maxTime = -1;
        File maxFile = null;

        for (File file : files) {
          long fileTime = Long.parseLong(file.getName());
          if(fileTime > afterTime && fileTime > maxTime) {
            maxTime = fileTime;
            maxFile = file;
          }
        }

        return maxFile;
      }
    }
  }

  public static class ReloadCacheRequestHandler extends RequestHandlerBase {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    @Override
    public void handleRequestBody(SolrQueryRequest req, SolrQueryResponse rsp) throws Exception {
      FileFloatSource.resetCache();
      log.debug("readerCache has been reset.");

      UpdateRequestProcessor processor =
          req.getCore().getUpdateProcessingChain(null).createProcessor(req, rsp);
      try {
        RequestHandlerUtils.handleCommit(req, processor, req.getParams(), true);
      } finally {
        try {
          processor.finish();
        } finally {
          processor.close();
        }
      }
    }

    @Override
    public String getDescription() {
      return "Reload readerCache request handler";
    }

    @Override
    public Name getPermissionName(AuthorizationContext request) {
      return Name.UPDATE_PERM;
    }
  }
}
