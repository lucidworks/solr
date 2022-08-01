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

import org.apache.solr.client.solrj.impl.CloudLegacySolrClient;
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
  private static final String FINAL_PARTITION_PREFIX = "partition_";
  private static final String MERGE_FILE_PREFIX = "merge_";
  private static final String SHARD_COMPLETE_FILE = "shard_complete";
  private static final int NUM_PARTITIONS = 11;
  private static final int SORT_PARTITION_SIZE = 50000;

  public static void main(String[] args) throws Exception{

    String inRoot = args[0];
    String outRoot = args[1];
    String zkHost = args[2];
    List<String> zkHosts = new ArrayList<>();
    zkHosts.add(zkHost);
    String mainCollection = args[3];

    CloudSolrClient solrClient = new CloudLegacySolrClient.Builder(zkHosts, Optional.empty()).build();

    try {
      solrClient.connect();
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
    return ExternalFile.iterate(root);
  }

  public static void process(ExternalFile externalFile, String outRoot, CloudSolrClient cloudSolrClient, String mainCollection) throws IOException {

    System.out.println("Processing External File:"+externalFile.file);

    if(!refresh(externalFile, outRoot)) {
      System.out.println("Old Data External File:"+externalFile.file);

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
      // Write the partition file
      new File(shardHome, SHARD_COMPLETE_FILE).createNewFile();
    }
  }

  public static List<File> sortPartition(File shardHome, int partitionNumber) throws IOException {
    DataInputStream partitionIn = null;
    List<Record> records = new ArrayList<>(SORT_PARTITION_SIZE);
    ByteComp byteComp = new ByteComp();
    File partitionFile = new File(shardHome, SHARD_TEMP_FILE + "." + partitionNumber);

    try {
      LinkedList<File> segments = new LinkedList<>();

      if(!partitionFile.exists()) {
        return segments;
      }

      partitionIn = new DataInputStream(new BufferedInputStream(new FileInputStream(partitionFile)));
      int segment = 0;
      boolean finished = false;
      while(!finished) {
        for (int i = 0; i < SORT_PARTITION_SIZE; i++) {
          try {
            Record record = new Record();
            record.read(partitionIn);
            records.add(record);
          } catch (EOFException E) {
            finished = true;
            break;
          }
        }

        if(records.size() > 0) {
          Collections.sort(records, byteComp);
          File segmentFile = new File(shardHome, SHARD_TEMP_FILE + "." + partitionIn + "." + segment);
          segments.addLast(segmentFile);
          writeSortedSegment(segmentFile, records);
          records.clear();
          ++segment;
        }
      }
      File finalSegment = mergeSegments(segments);
      finalSegment.renameTo(new File(finalSegment.getParentFile(), FINAL_PARTITION_PREFIX+partitionNumber));
      return segments;
    } finally {
      partitionFile.delete();
    }
  }

  public static void writeSortedSegment(File segmentFile, List<Record>records) throws IOException {

    DataOutputStream outSegment = null;

    try {
      outSegment = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(segmentFile)));
      for(Record record : records) {
        outSegment.writeByte(record.length);
        outSegment.write(record.bytes, 0, record.length);
        outSegment.writeFloat(record.f);
      }
    } finally {
      outSegment.close();
    }
  }

  public static File mergeSegments(LinkedList<File> segments) throws IOException {
    // Merge is a circular motion until there is only one segment.
    int mergeCount = 0;
    while(segments.size() > 1) {
      ++mergeCount;
      File file1 = segments.removeFirst();
      File file2 = segments.removeFirst();
      File file3 = merge(file1, file2, mergeCount);
      segments.addLast(file3);
      file1.delete();
      file2.delete();
    }

    File finalSegment = segments.removeLast();
    return finalSegment;
  }

  public static File merge(File file1, File file2, int mergeCount) throws IOException {
    byte[] file1Bytes = new byte[127];
    byte[] file2Bytes = new byte[127];

    DataInputStream file1In = null;
    DataInputStream file2In = null;
    DataOutputStream mergeOut = null;
    File mergeFile = new File(file1.getParentFile(), MERGE_FILE_PREFIX+Integer.toString(mergeCount));

    try {
      mergeOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(mergeFile)));
      file1In = new DataInputStream(new BufferedInputStream(new FileInputStream(file1), 50000));
      file2In = new DataInputStream(new BufferedInputStream(new FileInputStream(file2), 50000));
      byte length1 = file1In.readByte();
      file1In.read(file1Bytes, 0, length1);
      float f1 = file1In.readFloat();

      byte length2 = file2In.readByte();
      float f2 = file2In.readFloat();
      file2In.read(file2Bytes, 0, length2);

      boolean file1Done = false;
      boolean file2Done = false;

      while (!file1Done && !file2Done) {
        int value = compare(file1Bytes, length1, file1Bytes, length2);
        if (value == 0) {

          // We are equal write both
          // In the case of unique ID's this should not happen
          mergeOut.writeByte(length1);
          mergeOut.write(file1Bytes, 0, length1);
          mergeOut.writeFloat(f1);

          mergeOut.writeByte(length2);
          mergeOut.write(file2Bytes, 0, length2);
          mergeOut.writeFloat(f2);

          //Advance left
          try {
            length1 = file1In.readByte();
            file1In.read(file1Bytes, 0, length1);
            f1 = file1In.readFloat();
          } catch (EOFException eof) {
            file1Done = true;
          }
          //Advance right
          try {
            length2 = file2In.readByte();
            file2In.read(file2Bytes, 0, length2);
            f2 = file2In.readFloat();
          } catch (EOFException eof) {
            file2Done = true;
          }

        } else if (value < 1) {
          // Write and advance file1
          mergeOut.writeByte(length1);
          mergeOut.write(file1Bytes, 0, length1);
          mergeOut.writeFloat(f1);

          try {
            length1 = file1In.readByte();
            file1In.read(file1Bytes, 0, length1);
            f1 = file1In.readFloat();
          } catch (EOFException eof) {
            file1Done = true;
          }
        } else {
          // Write and advance file2
          mergeOut.writeByte(length2);
          mergeOut.write(file2Bytes, 0, length2);
          mergeOut.writeFloat(f2);
          try {
            length2 = file2In.readByte();
            file2In.read(file2Bytes, 0, length1);
            f2 = file2In.readFloat();
          } catch (EOFException eof) {
            file2Done = true;
          }
        }
      }

      //One of the files is done write out the other file
      if(!file1Done) {
        while (true) {

          mergeOut.writeByte(length1);
          mergeOut.write(file1Bytes, 0, length1);
          mergeOut.writeFloat(f1);

          try {

            length1 = file1In.readByte();
            file1In.read(file1Bytes, 0, length1);
            f1 = file1In.readFloat();

          } catch (EOFException eof) {
            // Do nothing
          }
        }
      }

      if(!file2Done) {
        while (true) {
          mergeOut.writeByte(length2);
          mergeOut.write(file2Bytes, 0, length2);
          mergeOut.writeFloat(f2);

          try {
            length2 = file2In.readByte();
            file2In.read(file2Bytes, 0, length1);
            f2 = file2In.readFloat();

          } catch (EOFException eof) {
            // Do nothing
          }
        }
      }
    } catch (Exception e) {
      file1In.close();
      file2In.close();
      mergeOut.close();
    }

    return mergeFile;
  }

  public static class Record {
    byte[] bytes;
    byte length;
    float f;

    public void read(DataInputStream dataInputStream) throws IOException {
      length = dataInputStream.readByte();
      bytes = new byte[length];
      dataInputStream.read(bytes, 0, length);
      f = dataInputStream.readFloat();
    }
  }

  public static class ByteComp implements Comparator<Record> {

    public int compare(Record rec1, Record rec2) {
      return ExternalFileUtil.compare(rec1.bytes, rec1.length, rec2.bytes, rec2.length);
    }
  }

  public static int compare(byte[] left, int length1, byte[] right, int length2) {

    for (int i = 0, j = 0; i < length1 && j < length2; i++, j++) {
      byte a = left[i];
      byte b = right[j];
      if (a != b) {
        return a - b;
      }
    }

    return length1 - length2;
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
          int bucket = Math.abs(hash % partitions.length);
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
          if(dataOutputStream != null) {
            dataOutputStream.close();
          }
        }
        tempFile.delete();
      }
    }
  }

  public static boolean refresh(ExternalFile externalFile, String outRoot) {
    File cdir = getCustomerOutDir(outRoot, externalFile);
    System.out.println("Customer outdir:"+cdir);

    if(!cdir.exists()) {
      cdir.mkdirs();
      return true;
    }

    String[] dirs = cdir.list();
    for(String dir : dirs) {
      long ldir = Long.parseLong(dir);
      if(externalFile.timeStamp > ldir) {
        return true;
      }
    }
    return false;
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

}