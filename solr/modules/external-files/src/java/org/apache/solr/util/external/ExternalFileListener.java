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

package org.apache.solr.util.external;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.solr.core.SolrEventListener;
import org.apache.solr.search.SolrIndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.lang.invoke.MethodHandles;

public class ExternalFileListener implements SolrEventListener {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String ID = "id";

  public void postCommit() {
  }

  public void postSoftCommit() {
  }

  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    DataOutputStream[] partitions = new DataOutputStream[ExternalFileUtil.NUM_PARTITIONS];
    String dataDir = newSearcher.getCore().getIndexDir();
    String newSearcherID = Integer.toHexString(newSearcher.hashCode());
    String currentSearcherID = Integer.toHexString(currentSearcher.hashCode());
    File dataDirFile = new File(dataDir);

    try {

      log.info("ExternalFileListener write file to {}", dataDirFile.getAbsolutePath());
      IndexReader reader = newSearcher.getTopReaderContext().reader();

      TermsEnum termsEnum = MultiTerms.getTerms(reader, ID).iterator();
      BytesRef bytesRef = null;
      PostingsEnum postingsEnum = null;
      int doc = -1;

      while ((bytesRef = termsEnum.next()) != null) {
        postingsEnum = termsEnum.postings(postingsEnum, PostingsEnum.NONE);
        while ((doc = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          int hash = ExternalFileUtil.hashCode(bytesRef.bytes, bytesRef.offset, bytesRef.length);
          int bucket = Math.abs(hash) % partitions.length;
          if (partitions[bucket] == null) {
            partitions[bucket] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(dataDirFile, newSearcherID + "_" + ExternalFileUtil.FINAL_PARTITION_PREFIX + Integer.toString(bucket)))));
          }
          partitions[bucket].writeByte(bytesRef.length);
          partitions[bucket].write(bytesRef.bytes, bytesRef.offset, bytesRef.length);
          partitions[bucket].writeInt(doc);
        }
      }
    } catch (Exception e) {
      log.error("ExternalFileListener Error", e);
    } finally {
      for (DataOutputStream dataOutputStream : partitions) {
        if (dataOutputStream != null) {
          try {
            dataOutputStream.close();
          } catch (Exception e1) {
            log.error("Error closing output stream.", e1);
          }
        }
      }

      //Clean up old files.
      String[] files = dataDirFile.list();
      for (String file : files) {
        if (file.contains(ExternalFileUtil.FINAL_PARTITION_PREFIX)) {
          if (!file.startsWith(newSearcherID) && !file.startsWith(currentSearcherID)) {
            new File(dataDirFile, file).delete();
          }
        }
      }
    }
  }
}