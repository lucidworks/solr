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

import java.io.*;

public class ExternalFileListener implements SolrEventListener {

  private static final String ID  = "id";

  public void postCommit() {}

  public void postSoftCommit() {}

  public void newSearcher(SolrIndexSearcher newSearcher, SolrIndexSearcher currentSearcher) {
    DataOutputStream[] partitions = new DataOutputStream[ExternalFileUtil.NUM_PARTITIONS];
    try {
      String dataDir = newSearcher.getCore().getCoreDescriptor().getDataDir();
      IndexReader reader = newSearcher.getTopReaderContext().reader();

      TermsEnum termsEnum = MultiTerms.getTerms(reader, ID).iterator();
      BytesRef bytesRef = null;
      PostingsEnum postingsEnum = null;
      int doc = -1;

      while((bytesRef = termsEnum.next()) != null) {
        termsEnum.postings(postingsEnum);
        while ((doc = postingsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
          int hash = ExternalFileUtil.hashCode(bytesRef.bytes, bytesRef.offset, bytesRef.length);
          int bucket = Math.abs(hash) % partitions.length;
          if(partitions[bucket] == null) {
            partitions[bucket] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(dataDir, ExternalFileUtil.FINAL_PARTITION_PREFIX+"."+bucket))));
          }
          partitions[bucket].writeByte(bytesRef.length);
          partitions[bucket].write(bytesRef.bytes, bytesRef.offset, bytesRef.length);
          partitions[bucket].writeInt(doc);
        }
      }
    } catch (Exception e) {
      for(DataOutputStream dataOutputStream : partitions) {
        if(dataOutputStream != null) {
          try {
            dataOutputStream.close();
          } catch (Exception e1) {
            //Log it.
          }
        }
      }
    }
  }
}