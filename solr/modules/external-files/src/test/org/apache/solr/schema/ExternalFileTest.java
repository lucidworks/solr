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
package org.apache.solr.schema;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Map;
import java.util.HashMap;
import java.util.Random;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.LuceneTestCase.Slow;
import org.apache.solr.SolrTestCaseJ4;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.SolrCloudTestCase;

import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.util.external.ExternalFileUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@Slow
@SolrTestCaseJ4.SuppressSSL
@LuceneTestCase.SuppressCodecs({"Lucene3x", "Lucene40", "Lucene41", "Lucene42", "Lucene45"})
public class ExternalFileTest extends SolrCloudTestCase {

  private static final String COLLECTIONORALIAS = "collection1";
  private static final int TIMEOUT = DEFAULT_TIMEOUT;
  private static final String id = "id";

  private static boolean useAlias;

  @BeforeClass
  public static void setupCluster() throws Exception {
    configureCluster(4).addConfig("conf", configset("ext")).configure();

    String collection;
    useAlias = random().nextBoolean();
    useAlias = false;
    if (useAlias) {
      collection = COLLECTIONORALIAS + "_collection";
    } else {
      collection = COLLECTIONORALIAS;
    }

    CollectionAdminRequest.createCollection(collection, "conf", 2, 1)
        .setPerReplicaState(SolrCloudTestCase.USE_PER_REPLICA_STATE)
        .process(cluster.getSolrClient());
    cluster.waitForActiveCollection(collection, 2, 2);
    if (useAlias) {
      CollectionAdminRequest.createAlias(COLLECTIONORALIAS, collection)
          .process(cluster.getSolrClient());
    }
  }

  @Before
  public void cleanIndex() throws Exception {
    new UpdateRequest().deleteByQuery("*:*").commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }

  @Test
  public void testExternal() throws Exception {

    Map<String, Float> pairs = new HashMap<>();
    Random rand = random();
    // Generate 100 random id->float pairs
    while(pairs.size() < 100) {
      float f = rand.nextFloat();
      pairs.put(Float.toHexString(f), f);
    }

    //Index those pairs.
    UpdateRequest updateRequest = new UpdateRequest();
    for(Map.Entry<String, Float> pair : pairs.entrySet()) {
      String id = pair.getKey();
      String fl = pair.getValue().toString();
      updateRequest.add("id", id, "test_f", fl);
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);
    String zkHost = cluster.getZkClient().getZkServerAddress();

    //Construct the expected raw directories
    File rawDirRoot = new File(cluster.getBaseDir().toFile(), "raw");
    File dataDir = new File(new File(new File(rawDirRoot, "bucket1"), "test_ef"), String.valueOf(System.currentTimeMillis()));
    dataDir.mkdirs();
    File dataFile = new File(dataDir, "test_ef.txt");
    PrintWriter out = new PrintWriter(new FileWriter(dataFile));
    try {
      for(Map.Entry<String, Float> pair : pairs.entrySet()) {
        String id = pair.getKey();
        String fl = pair.getValue().toString();
        out.println(id+":"+fl);
      }
    } finally {
      out.close();
    }

    long incTime = System.currentTimeMillis()+1;
    // Add an incremental file
    File incrementalFile = new File(dataDir.getParentFile(), "test_ef-"+String.valueOf(incTime)+".inc");
    int c = 0;
    Map<String, Float> incrementalMap = new HashMap<>();
    PrintWriter incWriter = null;
    try {
      incWriter = new PrintWriter(new FileWriter(incrementalFile));
      for (Map.Entry<String, Float> pair : pairs.entrySet()) {
        ++c;
        String id = pair.getKey();
        float f = rand.nextFloat();
        incrementalMap.put(id, f);
        incWriter.println(id+":"+f);
        if (c == 3) {
          break;
        }
      }
    } finally {
      incWriter.close();
    }

    // Create a second incremental file which overwrites one record in the first incremental
    File incrementalFile2 = new File(dataDir.getParentFile(), "test_ef-"+String.valueOf(incTime+1)+".inc");
    int c2 = 0;
    Map<String, Float> incrementalMap2 = new HashMap<>();
    PrintWriter incWriter2 = null;
    try {
      incWriter2 = new PrintWriter(new FileWriter(incrementalFile2));
      for (Map.Entry<String, Float> pair : incrementalMap.entrySet()) {
        ++c2;
        String id = pair.getKey();
        float f = rand.nextFloat();
        incrementalMap2.put(id, f);
        incWriter2.println(id+":"+f);
        if (c2 == 1) {
          break;
        }
      }
    } finally {
      incWriter2.close();
    }

    // Process the raw file
    File outRoot = new File(cluster.getBaseDir().toFile(), "out");

    //Assert the incremental files exists
    assertTrue(incrementalFile.exists());
    assertTrue(incrementalFile2.exists());

    // Set the EXTERNAL_ROOT_PATH_VAR needed for the load.
    System.setProperty(ExternalFileField2.EXTERNAL_ROOT_PATH_VAR, outRoot.getAbsolutePath());

    String[] args = {rawDirRoot.getAbsolutePath(), outRoot.getAbsolutePath(), zkHost, COLLECTIONORALIAS};
    ExternalFileUtil.main(args);

    //Assert that the incremental is cleaned up
    assertTrue(!incrementalFile.exists());
    assertTrue(!incrementalFile2.exists());

    SolrParams params = params("q", "*:*", "rows", "250", "fl", "id,test_f,field(test_ef)");
    SolrClient client = cluster.getSolrClient();
    QueryRequest request = new QueryRequest(params);
    QueryResponse response = request.process(client, COLLECTIONORALIAS);
    SolrDocumentList documentList = response.getResults();
    assertEquals(documentList.getNumFound(), pairs.size());

    int icount = 0;
    int i2count = 0;

    for (int i = 0; i < documentList.size(); i++) {

      SolrDocument document = documentList.get(i);
      String id = (String)document.getFieldValue("id");
      float f1 = (float)document.getFieldValue("test_f");
      float f2 = (float)document.getFieldValue("field(test_ef)");

      if (incrementalMap2.containsKey(id)) {
        assertEquals(incrementalMap2.get(id), f2, 0);
        assertNotEquals(incrementalMap.get(id), incrementalMap2.get(id), 0.0);
        assertNotEquals(f1, f2, 0.0);
        ++i2count;
      } else if (incrementalMap.containsKey(id)) {
        assertEquals(incrementalMap.get(id), f2, 0);
        assertNotEquals(f1, f2, 0.0);
        ++icount;
      } else {
        assertEquals(pairs.get(id), f1, 0);
        assertEquals(f1, f2, 0.0);
      }
    }

    assertEquals(i2count, 1);
    assertEquals(icount, 2);
  }

  @Test
  public void testSparseIndex() throws Exception {

    Map<String, Float> pairs = new HashMap<>();
    Random rand = random();
    // Generate 100 random id->float pairs
    while(pairs.size() < 500) {
      float f = rand.nextFloat();
      pairs.put(Float.toHexString(f), f);
    }

    //Index those pairs.
    UpdateRequest updateRequest = new UpdateRequest();
    Map<String, Float> sparseIndex = new HashMap<>();
    for(Map.Entry<String, Float> pair : pairs.entrySet()) {
      int i = rand.nextInt();
      if(i % 2 == 0) {
        String id = pair.getKey();
        String fl = pair.getValue().toString();
        updateRequest.add("id", id, "test_f", fl);
        sparseIndex.put(id, pair.getValue());
      }
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);
    String zkHost = cluster.getZkClient().getZkServerAddress();

    //Construct the expected raw directories
    File rawDirRoot = new File(cluster.getBaseDir().toFile(), "raw");
    File dataDir = new File(new File(new File(rawDirRoot, "bucket1"), "test_ef"), String.valueOf(System.currentTimeMillis()));
    dataDir.mkdirs();
    File dataFile = new File(dataDir, "test_ef.txt");
    PrintWriter out = new PrintWriter(new FileWriter(dataFile));
    try {
      for(Map.Entry<String, Float> pair : pairs.entrySet()) {
        String id = pair.getKey();
        String fl = pair.getValue().toString();
        out.println(id+":"+fl);
      }
    } finally {
      out.close();
    }

    // Process the raw file
    File outRoot = new File(cluster.getBaseDir().toFile(), "out");

    // Set the EXTERNAL_ROOT_PATH_VAR needed for the load.
    System.setProperty(ExternalFileField2.EXTERNAL_ROOT_PATH_VAR, outRoot.getAbsolutePath());

    String[] args = {rawDirRoot.getAbsolutePath(), outRoot.getAbsolutePath(), zkHost, COLLECTIONORALIAS};
    ExternalFileUtil.main(args);

    SolrParams params = params("q", "*:*", "rows", "500", "fl", "id,test_f,field(test_ef)");
    SolrClient client = cluster.getSolrClient();
    QueryRequest request = new QueryRequest(params);
    QueryResponse response = request.process(client, COLLECTIONORALIAS);
    SolrDocumentList documentList = response.getResults();
    assertEquals(documentList.getNumFound(), sparseIndex.size());

    for(int i=0; i < documentList.size(); i++) {
      SolrDocument document = documentList.get(i);
      String id = (String)document.getFieldValue("id");
      float f1 = (float)document.getFieldValue("test_f");
      float f2 = (float)document.getFieldValue("field(test_ef)");
      assertEquals(pairs.get(id), f1, 0);
      assertEquals(f1, f2, 0.0);
    }
  }

  @Test
  public void testSparseExternal() throws Exception {

    Map<String, Float> pairs = new HashMap<>();
    Random rand = random();
    // Generate 100 random id->float pairs
    while(pairs.size() < 500) {
      float f = rand.nextFloat();
      pairs.put(Float.toHexString(f), f);
    }

    //Index those pairs.
    UpdateRequest updateRequest = new UpdateRequest();
    for(Map.Entry<String, Float> pair : pairs.entrySet()) {
      String id = pair.getKey();
      String fl = pair.getValue().toString();
      updateRequest.add("id", id, "test_f", fl);
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);
    String zkHost = cluster.getZkClient().getZkServerAddress();

    //Construct the expected raw directories
    File rawDirRoot = new File(cluster.getBaseDir().toFile(), "raw");
    File dataDir = new File(new File(new File(rawDirRoot, "bucket1"), "test_ef"), String.valueOf(System.currentTimeMillis()));
    dataDir.mkdirs();
    File dataFile = new File(dataDir, "test_ef.txt");
    PrintWriter out = new PrintWriter(new FileWriter(dataFile));
    Map<String, Float> sparseMap = new HashMap<>();
    try {
      for(Map.Entry<String, Float> pair : pairs.entrySet()) {
        int i = rand.nextInt();
        if(i % 2 == 0) {
          String id = pair.getKey();
          String fl = pair.getValue().toString();
          out.println(id + ":" + fl);
          sparseMap.put(id,pair.getValue());
        }
      }
    } finally {
      out.close();
    }

    // Process the raw file
    File outRoot = new File(cluster.getBaseDir().toFile(), "out");

    // Set the EXTERNAL_ROOT_PATH_VAR needed for the load.
    System.setProperty(ExternalFileField2.EXTERNAL_ROOT_PATH_VAR, outRoot.getAbsolutePath());

    String[] args = {rawDirRoot.getAbsolutePath(), outRoot.getAbsolutePath(), zkHost, COLLECTIONORALIAS};
    ExternalFileUtil.main(args);

    SolrParams params = params("q", "*:*", "rows", "500", "fl", "id,test_f,field(test_ef)");
    SolrClient client = cluster.getSolrClient();
    QueryRequest request = new QueryRequest(params);
    QueryResponse response = request.process(client, COLLECTIONORALIAS);
    SolrDocumentList documentList = response.getResults();
    assertEquals(documentList.getNumFound(), pairs.size());

    int s = 0;
    for(int i=0; i < documentList.size(); i++) {
      SolrDocument document = documentList.get(i);
      String id = (String)document.getFieldValue("id");
      float f1 = (float)document.getFieldValue("test_f");
      float f2 = (float)document.getFieldValue("field(test_ef)");
      if(sparseMap.containsKey(id)) {
        ++s;
        assertEquals(pairs.get(id), f1, 0);
        assertEquals(f1, f2, 0.0);
      } else {
        assertEquals(f2, 0.0, 0);
      }
    }
    assertEquals(s, sparseMap.size());
  }

  @Test
  public void testSparse() throws Exception {

    Map<String, Float> pairs = new HashMap<>();
    Random rand = random();
    // Generate 100 random id->float pairs
    while(pairs.size() < 500) {
      float f = rand.nextFloat();
      pairs.put(Float.toHexString(f), f);
    }

    //Index those pairs.
    UpdateRequest updateRequest = new UpdateRequest();
    Map<String, Float> sparseIndex = new HashMap<>();
    for(Map.Entry<String, Float> pair : pairs.entrySet()) {
      int i = rand.nextInt();
      if(i % 2 == 0) {
        String id = pair.getKey();
        String fl = pair.getValue().toString();
        updateRequest.add("id", id, "test_f", fl);
        sparseIndex.put(id, pair.getValue());
      }
    }

    updateRequest.commit(cluster.getSolrClient(), COLLECTIONORALIAS);
    String zkHost = cluster.getZkClient().getZkServerAddress();

    //Construct the expected raw directories
    File rawDirRoot = new File(cluster.getBaseDir().toFile(), "raw");
    File dataDir = new File(new File(new File(rawDirRoot, "bucket1"), "test_ef"), String.valueOf(System.currentTimeMillis()));
    dataDir.mkdirs();
    File dataFile = new File(dataDir, "test_ef.txt");
    PrintWriter out = new PrintWriter(new FileWriter(dataFile));
    Map<String, Float> sparseMap = new HashMap<>();
    try {
      for(Map.Entry<String, Float> pair : pairs.entrySet()) {
        int i = rand.nextInt();
        if(i % 2 == 0) {
          String id = pair.getKey();
          String fl = pair.getValue().toString();
          out.println(id + ":" + fl);
          sparseMap.put(id,pair.getValue());
        }
      }
    } finally {
      out.close();
    }

    // Process the raw file
    File outRoot = new File(cluster.getBaseDir().toFile(), "out");

    // Set the EXTERNAL_ROOT_PATH_VAR needed for the load.
    System.setProperty(ExternalFileField2.EXTERNAL_ROOT_PATH_VAR, outRoot.getAbsolutePath());

    String[] args = {rawDirRoot.getAbsolutePath(), outRoot.getAbsolutePath(), zkHost, COLLECTIONORALIAS};
    ExternalFileUtil.main(args);

    SolrParams params = params("q", "*:*", "rows", "500", "fl", "id,test_f,field(test_ef)");
    SolrClient client = cluster.getSolrClient();
    QueryRequest request = new QueryRequest(params);
    QueryResponse response = request.process(client, COLLECTIONORALIAS);
    SolrDocumentList documentList = response.getResults();
    assertEquals(documentList.getNumFound(), sparseIndex.size());

    int s = 0;
    for(String key : sparseIndex.keySet()) {
      if(sparseMap.containsKey(key)) {
        ++s;
      }
    }
    int t = 0;
    for(int i=0; i < documentList.size(); i++) {
      SolrDocument document = documentList.get(i);
      String id = (String)document.getFieldValue("id");
      float f1 = (float) document.getFieldValue("test_f");
      float f2 = (float) document.getFieldValue("field(test_ef)");
      if (sparseMap.containsKey(id)) {
        ++t;
        assertEquals(pairs.get(id), f1, 0);
        assertEquals(f1, f2, 0.0);
      } else {
        assertEquals(f2, 0.0, 0);
      }
    }

    assertTrue(s > 0);
    assertEquals(s,t);
  }

}