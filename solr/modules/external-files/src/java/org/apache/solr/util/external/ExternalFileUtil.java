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
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.util.Hash;

import java.io.*;
import java.util.*;

public class ExternalFileUtil {

  private static final int HASH_SEED = 12131344;
  private static final int HASH_DIRS = 250;
  private static final String SHARD_TEMP_FILE = "temp.bin";
  private static final int NUM_PARTITIONS = 11;
  private static final int SORT_PARTITION_SIZE = 50000;

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

  public static void process(ExternalFile externalFile, String outRoot, CloudSolrClient cloudSolrClient, String mainCollection) throws IOException {

    if(oldData(externalFile, outRoot)) {
      return;
    }

    DocCollection docCollection = cloudSolrClient.getClusterState().getCollection(mainCollection);
    DocRouter docRouter = docCollection.getRouter();
    BufferedReader in = null;
    //Split the file by shard.
    Map<String, DataOutputStream> shardOuts = new HashMap<>();
    List<File> shardHomes = new ArrayList<>();
    try {
      in = new BufferedReader(new FileReader(externalFile.file));
      String line = null;
      while((line = in.readLine()) != null) {
        String[] pair = line.split(":");
        String id = pair[0].trim();
        float f = Float.parseFloat(pair[1]);
        Slice slice = docRouter.getTargetSlice(id, null, null, null, docCollection);
        String shardId = slice.getName();
        if(shardOuts.containsKey(shardId)) {
          DataOutputStream shardOut = shardOuts.get(shardId);
          byte[] bytes = id.getBytes();
          shardOut.writeByte(bytes.length);
          shardOut.write(bytes);
          shardOut.writeFloat(f);
        } else {
          DataOutputStream shardOut = openShardOut(outRoot, externalFile, shardId, SHARD_TEMP_FILE);
          shardHomes.add(getShardHome(outRoot, externalFile, shardId));
          byte[] bytes = id.getBytes();
          shardOut.writeByte(bytes.length);
          shardOut.write(bytes);
          shardOut.writeFloat(f);
          shardOuts.put(shardId, shardOut);
        }
      }
    } finally {
      in.close();
      for(DataOutputStream dataOutputStream : shardOuts.values()) {
        dataOutputStream.close();
      }
    }

    partitionShards(shardHomes);
    sortPartitions(shardHomes);
  }

  public static void sortPartitions(List<File> shardHomes) throws IOException {
    for(File shardHome : shardHomes) {
      for(int i=0; i<NUM_PARTITIONS; i++) {
        sortPartition(shardHome, i);
      }
    }
  }

  public static void sortPartition(File shardHome, int partitionNumber) throws IOException {
    DataInputStream partitionIn = null;
    List<Record> records = new ArrayList<>(SORT_PARTITION_SIZE);
    ByteComp byteComp = new ByteComp();
    try {
      partitionIn = new DataInputStream(new BufferedInputStream(new FileInputStream(new File(shardHome, SHARD_TEMP_FILE + "." + partitionNumber))));
      while(true) {
        for (int i = 0; i < SORT_PARTITION_SIZE; i++) {
          Record record = new Record();
          record.read(partitionIn);
          records.add(record);
        }
        Collections.sort(records, byteComp);
        

      }

    } finally {

    }
  }

  public static class Record  {
    byte[] bytes;
    int length;
    float f;

    public void read(DataInputStream dataInputStream) throws IOException {
      length = dataInputStream.read();
      bytes = new byte[length];
      dataInputStream.read(bytes, 0, length);
      f = dataInputStream.readFloat();
    }
  }

  public static class ByteComp implements Comparator<Record> {

    public int compare(Record rec1, Record rec2) {
      return compare(rec1.bytes, rec1.length, rec2.bytes, rec2.length);
    }

    int compare(byte[] left, int length1, byte[] right, int length2) {
      for (int i = 0, j = 0; i < length1 && j < length2; i++, j++) {
        byte a = left[i];
        byte b = right[j];
        if (a != b) {
          return a - b;
        }
      }
      return length1 - length2;
    }
  }


  public static void partitionShards(List<File> shardHomes) throws IOException {
    //Process the shard files
    byte[] bytes = new byte[128];

    for(File shardHome : shardHomes) {
      DataOutputStream[] partitions = new DataOutputStream[NUM_PARTITIONS];
      DataInputStream tempStream = null;
      File tempFile = new File(shardHome, SHARD_TEMP_FILE);
      try {
        tempStream = new DataInputStream(new BufferedInputStream(new FileInputStream(tempFile)));
        while (true) {
          byte b = tempStream.readByte();
          tempStream.read(bytes, 0, b);
          float f = tempStream.readFloat();
          int hash = Hash.murmurhash3_x86_32(bytes, 0, b, HASH_SEED);
          int bucket = hash % partitions.length;
          if(partitions[bucket] == null) {
            partitions[bucket] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(shardHome, SHARD_TEMP_FILE+"."+bucket))));
          }

          partitions[bucket].writeByte(b);
          partitions[bucket].write(bytes, 0, b);
          partitions[bucket].writeFloat(f);
        }
      } catch (EOFException e) {
        //File ended do nothing.
      } finally {
        tempStream.close();
        for(DataOutputStream dataOutputStream : partitions) {
          dataOutputStream.close();
        }
        tempFile.delete();
      }
    }
  }

  public static boolean oldData(ExternalFile externalFile, String outRoot) {
    File cdir = getCustomerOutDir(outRoot, externalFile);

    if(!cdir.exists()) {
      cdir.mkdirs();
      return false;
    }

    String[] dirs = cdir.list();
    for(String dir : dirs) {
      long ldir = Long.parseLong(dir);
      if(externalFile.timeStamp > ldir) {
        return false;
      }
    }
    return true;
  }

  public static String getHashDir(String customer) {
    int bucket = customer.hashCode() % HASH_DIRS;
    return "bucket"+bucket;
  }

  public static File getCustomerOutDir(String outRoot, ExternalFile externalFile) {
    return new File(new File(new File(new File(outRoot), getHashDir(externalFile.customer)), externalFile.customer), externalFile.type);
  }

  public static DataOutputStream openShardOut(String outRoot, ExternalFile externalFile, String shardId, String fileName) throws IOException {
    return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(getShardHome(outRoot, externalFile, shardId), fileName))));
  }

  public static File getShardHome(String outRoot, ExternalFile externalFile, String shardId) {
    File custDir = getCustomerOutDir(outRoot, externalFile);
    File file = new File(new File(custDir, Long.toString(externalFile.timeStamp)), shardId);
    if(!file.exists()) {
      file.mkdirs();
    }
    return file;
  }

  public static String getShard() {
    return null;
  }
}