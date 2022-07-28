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

import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.DocRouter;
import org.apache.solr.common.util.Hash;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ExternalFileUtil {

  private static int HASH_SEED = 12131344;

  public static void main(String[] args) throws Exception{

    String inRoot = args[0];
    String outRoot = args[1];
    List<String> zkHosts = new ArrayList<>();
    String mainCollection = args[2];

    CloudSolrClient solrClient = new CloudSolrClient.Builder(zkHosts).build();

    try {
      Iterator<ExternalFile> iterator = iterate(inRoot);
      while (iterator.hasNext()) {
        ExternalFile externalFile = iterator.next();
        process(externalFile, outRoot, solrClient, mainCollection);
      }
    } finally {
      solrClient.close();
    }
  }

  public static int hashCode(byte[] bytes, int offset, int length) {
    return Hash.murmurhash3_x86_32(bytes, offset, length, HASH_SEED);
  }

  public static Iterator<ExternalFile> iterate(String root) {
    return null;
  }

  public static void process(ExternalFile externalFile, String outRoot, CloudSolrClient cloudSolrClient, String mainCollection) {
    DocCollection docCollection = cloudSolrClient.getClusterState().getCollection(mainCollection);
    DocRouter docRouter = docCollection.getRouter();


  }

  public static String getShard() {
    return null;
  }
}