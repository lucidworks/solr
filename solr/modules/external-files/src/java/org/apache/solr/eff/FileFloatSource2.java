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
package org.apache.solr.eff;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.queries.function.docvalues.FloatDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.search.SolrCache;
import org.apache.solr.util.VersionedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Obtains float field values from an external file.
 *
 * @see ExternalFileField2
 */

public class FileFloatSource2 extends ValueSource {

    private final SchemaField field;
    private final SchemaField keyField;
    private final float defVal;
    private final String dataDir;

    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    /**
     * Creates a new FileFloatSource2
     * @param field the source's SchemaField
     * @param keyField the field to use as a key
     * @param defVal the default value to use if a field has no entry in the external file
     * @param datadir the directory in which to look for the external file
     */
    public FileFloatSource2(SchemaField field, SchemaField keyField, float defVal, String datadir) {
        this.field = field;
        this.keyField = keyField;
        this.defVal = defVal;
        this.dataDir = datadir;
    }

    @Override
    public String description() {
        return "float(" + field + ')';
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    public FunctionValues getValues(Map context, LeafReaderContext readerContext) throws IOException {
        final int off = readerContext.docBase;
        IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(readerContext);

        final float[] arr = getCachedFloats(topLevelContext.reader());
        return new FloatDocValues(this) {

            @Override
            public float floatVal(int doc) {
                // `arr` is empty if there is no file available
                return (arr.length == 0) ? defVal : arr[doc + off];
            }

            @Override
            public Object objectVal(int doc) {
                return floatVal(doc);
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (o.getClass() !=  FileFloatSource2.class) return false;
        FileFloatSource2 other = (FileFloatSource2)o;
        return this.field.getName().equals(other.field.getName())
                && this.keyField.getName().equals(other.keyField.getName())
                && this.defVal == other.defVal
                && this.dataDir.equals(other.dataDir);
    }

    @Override
    public int hashCode() {
        return FileFloatSource2.class.hashCode() + field.getName().hashCode();
    }

    @Override
    public String toString() {
        return "FileFloatSource2(field="+field.getName()+",keyField="+keyField.getName()
                + ",defVal="+defVal+",dataDir="+dataDir+")";

    }

    private float[] getCachedFloats(IndexReader reader) {
        Entry key = new Entry(this);

        String cacheName = "fileFloatSourceCache_" + field.getType().getTypeName();

        // SolrCache is named by the field type name so dynamic field patterns pointing to the same type will share the same cache
        @SuppressWarnings("unchecked")
        final SolrCache<Entry,float[]> cache =
                SolrRequestInfo.getRequestInfo().getReq().getSearcher().getCache(cacheName);

        if (cache == null) {
            throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Cache '" + cacheName + "' must be defined to use this field type and value source");
        }

        float[] floats;
        try {
            floats = cache.computeIfAbsent(key, k -> getFloats(key.ffs, reader));
        } catch (IOException e) {
            throw new UncheckedIOException(e); // Shouldn't happen because nothing in here throws
        }

        return floats;
    }

    /** Expert: Every composite-key in the internal cache is of this type. */
    public static class Entry {
        public final FileFloatSource2 ffs;
        public Entry(FileFloatSource2 ffs) {
            this.ffs = ffs;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Entry)) return false;
            Entry other = (Entry)o;
            return ffs.equals(other.ffs);
        }

        @Override
        public int hashCode() {
            return ffs.hashCode();
        }
    }

    public static float[] getFloats(FileFloatSource2 ffs, IndexReader reader) {
        InputStream is;
        String fname = "external_" + ffs.field.getName();
        try {
            is = VersionedFile.getLatestFile(ffs.dataDir, fname);
        } catch (IOException e) {
            // log, use defaults
            log.warn("Error opening external value source file: ", e);
            return new float[0];
        }

        float[] vals = new float[reader.maxDoc()];
        if (ffs.defVal != 0) {
            Arrays.fill(vals, ffs.defVal);
        }

        BufferedReader r = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));

        String idName = ffs.keyField.getName();
        FieldType idType = ffs.keyField.getType();

        // warning: lucene's termEnum.skipTo() is not optimized... it simply does a next()
        // because of this, simply ask the reader for a new termEnum rather than
        // trying to use skipTo()

        List<String> notFound = new ArrayList<>();
        int notFoundCount=0;
        int otherErrors=0;

        char delimiter='=';

        BytesRefBuilder internalKey = new BytesRefBuilder();

        try {
            TermsEnum termsEnum = MultiTerms.getTerms(reader, idName).iterator();
            PostingsEnum postingsEnum = null;

            // removing deleted docs shouldn't matter
            // final Bits liveDocs = MultiLeafReader.getLiveDocs(reader);

            for (String line; (line=r.readLine())!=null;) {
                int delimIndex = line.lastIndexOf(delimiter);
                if (delimIndex < 0) continue;

                int endIndex = line.length();
                String key = line.substring(0, delimIndex);
                String val = line.substring(delimIndex+1, endIndex);

                float fval;
                try {
                    idType.readableToIndexed(key, internalKey);
                    fval=Float.parseFloat(val);
                } catch (Exception e) {
                    if (++otherErrors<=10) {
                        log.error("Error loading external value source + fileName + {}{}", e
                                , (otherErrors < 10 ? "" : "\tSkipping future errors for this file."));
                    }
                    continue;  // go to next line in file.. leave values as default.
                }

                if (!termsEnum.seekExact(internalKey.get())) {
                    if (notFoundCount<10) {  // collect first 10 not found for logging
                        notFound.add(key);
                    }
                    notFoundCount++;
                    continue;
                }

                postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
                int doc;
                while ((doc = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    vals[doc] = fval;
                }
            }

        } catch (IOException e) {
            // log, use defaults
            log.error("Error loading external value source: ", e);
        } finally {
            // swallow exceptions on close so we don't override any
            // exceptions that happened in the loop
            try{r.close();}catch(Exception e){}
        }
        if (log.isInfoEnabled()) {
            String tmp = (notFoundCount == 0 ? "" : " :" + notFoundCount + " missing keys " + notFound);
            log.info("Loaded external value source {}{}", fname, tmp);
        }
        return vals;
    }
}
