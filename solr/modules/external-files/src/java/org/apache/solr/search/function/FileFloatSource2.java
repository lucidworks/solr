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
  private final File dataDir;
  private final File externalDir;
  private final String fileNameStripped;
  private final String searcherId;
  private final String shardId;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Creates a new FileFloatSource2
   *
   * @param field the source's SchemaField
   * @param keyField the field to use as a key
   * @param defVal the default value to use if a field has no entry in the external file
   * @param dataDir the directory in which to look for the external file
   */
  public FileFloatSource2(SchemaField field, SchemaField keyField, float defVal, String dataDir, String externalDir, String searcherId, String shardId) {
    this.field = field;
    this.fileNameStripped = field.getName().replace("_ef$", "");
    this.keyField = keyField;
    this.defVal = defVal;
    this.dataDir = new File(new File(dataDir), "external");
    this.externalDir = new File(externalDir);
    this.searcherId = searcherId;
    this.shardId = shardId;

    log.info("Constructing FileFloatSource2");
    log.info("field={}", field.getName());
    log.info("fileNameStripped={}", fileNameStripped);
    log.info("keyField={}", keyField.getName());
    log.info("defVal={}", Float.toString(defVal));
    log.info("dataDir={}", this.dataDir.getAbsolutePath());
    log.info("externalDir={}", this.externalDir.getAbsolutePath());
    log.info("searchId={}", searcherId);
    log.info("shardId={}", shardId);
  }

  @Override
  public String description() {
    return "float(" + field + ')';
  }

  @SuppressWarnings("unchecked")
  @Override
  public FunctionValues getValues(Map<Object, Object> context, LeafReaderContext readerContext)
      throws IOException {

    System.out.println("HERE FileFloatSource2.getValues()");

    float[] arr = null;
    Object o = context.get(getClass().getName());
    Map<String, float[]> floatMap = null;
    if(o instanceof Map) {

      floatMap = (Map<String, float[]>)o;
      arr = floatMap.get(this.fileNameStripped);
    } else {
      floatMap = new HashMap<>();
      context.put(getClass().getName(), floatMap);
    }


    if(arr == null) {
      log.info("FileFloatSource2.getValues() from cache");
      IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(readerContext);
      arr = getCachedFloats(topLevelContext.reader());
      if(arr != null) {
        floatMap.put(this.fileNameStripped, arr);
      }
    }

    if(arr != null) {

      final float[] vals = arr;
      final int off = readerContext.docBase;
      return new FloatDocValues(this) {
        @Override
        public float floatVal(int doc) {
          return vals[doc + off];
        }

        @Override
        public Object objectVal(int doc) {
          return floatVal(doc); // TODO: keep track of missing values
        }
      };

    } else {

      return new FloatDocValues(this) {
        @Override
        public float floatVal(int doc) {
          return defVal;
        }

        @Override
        public Object objectVal(int doc) {
          return floatVal(doc); // TODO: keep track of missing values
        }
      };

    }
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
    log.info("FileFloatSource2 Getting cached floats");
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

        value = null;

        if (value == null) {
          value = new CreationPlaceholder();
          innerCache.put(key, value);
        } else {
          cachedFloats = (CachedFloats) value;
          if (timeStamp - cachedFloats.lastChecked > cachedFloats.ttl) {
            // ttl has expired so force a check of the files on disk.
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
                // Cached floats were loaded above and were found to be the up-to-date with disk version
                // Reset the lastChecked.
                cachedFloats.lastChecked = timeStamp;
                // Return the cached floats
                synchronized (readerCache) {
                  innerCache.put(key, cachedFloats);
                }
                return cachedFloats.floats;
              } else {
                // Cached floats were not loaded possible there was no file?
                // Remove the placeholder
                synchronized (readerCache) {
                  innerCache.remove(key);
                }
                return null;
              }
            } else {
              // Floats were loaded from the file
              CachedFloats _cachedFloats = new CachedFloats();
              _cachedFloats.floats = (float[]) progress.value;
              _cachedFloats.lastChecked = _cachedFloats.loadTime = timeStamp;
              synchronized (readerCache) {
                innerCache.put(key, _cachedFloats);
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

    File latestExternalDir = getLatestFileDir(
        ffs.externalDir,
        ffs.fileNameStripped,
        ffs.shardId,
        cachedFloats == null ? -1 : cachedFloats.loadTime);

    if (latestExternalDir == null) {
      return null;
    }

    log.info("Got latest external file dir : {}", latestExternalDir.getAbsolutePath());

    ExecutorService executorService = ExecutorUtil.newMDCAwareCachedThreadPool(8, new SolrNamedThreadFactory("FileFloatSource2"));
    float[] vals = new float[reader.maxDoc()];
    if (ffs.defVal != 0) {
      Arrays.fill(vals, ffs.defVal);
    }

    long start = System.nanoTime();

    List<Future<?>> futures = new ArrayList<>();
    try {

      String externalFileBase = ffs.searcherId + "_" + ExternalFileUtil.FINAL_PARTITION_PREFIX;

      for (int i=0; i<8; i++) {
        MergeJoin merger = new MergeJoin(new File(ffs.dataDir, externalFileBase + i), new File(latestExternalDir, ExternalFileUtil.FINAL_PARTITION_PREFIX+i), vals);
        Future<?> future = executorService.submit(merger);
        futures.add(future);
      }

      for (Future<?> future : futures) {
        future.get();
      }

    } catch (Exception e) {
      log.error("Error loading floats", e);
    } finally {
      executorService.shutdown();;
    }

    long end = System.nanoTime();
    log.info("File load time: {}", Long.toString((end - start) / 1_000_000));
    return vals;
  }

  public static final class MergeJoin implements Runnable {

    private final File indexFile;
    private final File externalFile;
    private final float[] vals;

    public MergeJoin(File indexFile, File externalFile, float[] vals) {
      this.indexFile = indexFile;
      this.externalFile = externalFile;
      this.vals = vals;
    }

    private String toString(byte[] bytes, int length) {
      StringBuffer buf = new StringBuffer();
      for(int i=0; i<length; i++) {
        buf.append((char)bytes[i]);
      }
      return buf.toString();
    }

    public void run() {

      BufferedInputStream indexIn = null;
      BufferedInputStream externalIn = null;
      final byte[] indexBytes = new byte[127];
      final byte[] externalBytes = new byte[127];

      try {

        indexIn = new BufferedInputStream(new FileInputStream(indexFile), 131_072);
        externalIn = new BufferedInputStream(new FileInputStream(externalFile), 131_072);

        /*
        *  Load the first index record:
        *  Read 5 bytes. The first 4 bytes are the luceneId so construct the int. The 5th byte is the length of the unique id.
        *  Then load the bytes for for unique id.
        */

        int numRead = indexIn.read(indexBytes, 0, 5);
        if(numRead < 5) { return;}
        int luceneId = (((indexBytes[0] & 0xff) << 24) + ((indexBytes[1] & 0xff) << 16) + ((indexBytes[2] & 0xff) << 8) + ((indexBytes[3] & 0xff) << 0));
        int indexIdLength = indexBytes[4];
        indexIn.read(indexBytes, 0, indexIdLength);


        /*
         *  Load the first external record:
         *  Read 5 bytes. The first 4 bytes are the float so construct the float. The 5th byte is the length of the unique id.
         *  Then load the bytes for for unique id.
         */

        numRead = externalIn.read(externalBytes, 0, 5);
        if(numRead < 5) {return;}
        float fval = Float.intBitsToFloat(((externalBytes[0] & 0xff) << 24) + ((externalBytes[1] & 0xff) << 16) + ((externalBytes[2] & 0xff) << 8) + ((externalBytes[3] & 0xff) << 0));
        int externalIdLength = externalBytes[4];
        externalIn.read(externalBytes, 0, externalIdLength);

        int value;

        while (true) {

          value = ExternalFileUtil.compare(indexBytes, indexIdLength, externalBytes, externalIdLength);

          if (value == 0) {

            vals[luceneId] = fval;

            numRead = indexIn.read(indexBytes, 0, 5);
            if(numRead < 5) { return;}
            luceneId = (((indexBytes[0] & 0xff) << 24) + ((indexBytes[1] & 0xff) << 16) + ((indexBytes[2] & 0xff) << 8) + ((indexBytes[3] & 0xff) << 0));
            indexIdLength = indexBytes[4];
            indexIn.read(indexBytes, 0, indexIdLength);

            numRead = externalIn.read(externalBytes, 0, 5);
            if(numRead < 5) {return;}
            fval = Float.intBitsToFloat(((externalBytes[0] & 0xff) << 24) + ((externalBytes[1] & 0xff) << 16) + ((externalBytes[2] & 0xff) << 8) + ((externalBytes[3] & 0xff) << 0));
            externalIdLength = externalBytes[4];
            externalIn.read(externalBytes, 0, externalIdLength);

          } else if (value > 0) {
            // Advance only the external
            numRead = externalIn.read(externalBytes, 0, 5);
            if(numRead < 5) {return;}
            fval = Float.intBitsToFloat(((externalBytes[0] & 0xff) << 24) + ((externalBytes[1] & 0xff) << 16) + ((externalBytes[2] & 0xff) << 8) + ((externalBytes[3] & 0xff) << 0));
            externalIdLength = externalBytes[4];
            externalIn.read(externalBytes, 0, externalIdLength);
          } else {
            // Advance only the index
            numRead = indexIn.read(indexBytes, 0, 5);
            if(numRead < 5) { return;}
            luceneId = (((indexBytes[0] & 0xff) << 24) + ((indexBytes[1] & 0xff) << 16) + ((indexBytes[2] & 0xff) << 8) + ((indexBytes[3] & 0xff) << 0));
            indexIdLength = indexBytes[4];
            indexIn.read(indexBytes, 0, indexIdLength);
          }
        }
      } catch (EOFException f) {
        //Do nothing
      } catch (Exception e) {
        log.error("Join error", e);
        throw new RuntimeException(e);
      } finally {
        try {
          indexIn.close();
        } catch(Exception e) {

        }

        try {
          externalIn.close();
        } catch(Exception e) {

        }
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

  public static File getLatestFileDir(File root, String fileName, String shardId, long afterTime) {

    File fileHome = new File(new File(root, ExternalFileUtil.getHashDir(fileName)), fileName);
    if(!fileHome.exists()) {
      return null;
    } else {
      File[] files = fileHome.listFiles();
      if(files == null || files.length == 0) {
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

        if(maxFile != null) {
          //Check if finished writing
          File shardHome = new File(maxFile, shardId);
          File shardComplete = new File(shardHome, ExternalFileUtil.SHARD_COMPLETE_FILE);
          if (shardComplete.exists()) {
            return shardHome;
          } else {
            return null;
          }
        } else {
          return null;
        }
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
