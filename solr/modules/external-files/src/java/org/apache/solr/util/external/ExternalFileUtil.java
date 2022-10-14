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
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.*;

public class ExternalFileUtil {

  private static final int HASH_SEED = 12131344;
  private static final int HASH_DIRS = 250;
  private static final String SHARD_TEMP_FILE = "temp.bin";
  private static final String SEGMENT_TEMP_FILE = "segment.bin";
  public static final String FINAL_PARTITION_PREFIX = "partition_";
  private static final String MERGE_FILE_PREFIX = "merge_";
  public static final String SHARD_COMPLETE_FILE = "shard_complete";
  public static final int NUM_PARTITIONS = 8;
  private static final int SORT_PARTITION_SIZE = 50000;

  public static void main(String[] args) throws Exception {

    String inRoot = args[0];
    String outRoot = args[1];
    String zkHost = args[2];
    List<String> zkHosts = new ArrayList<>();
    zkHosts.add(zkHost);
    String mainCollection = args[3];

    CloudSolrClient solrClient = new CloudLegacySolrClient.Builder(zkHosts, Optional.empty()).build();
    RandomAccessFile raFile = null;
    FileChannel fileChannel = null;
    FileLock fileLock = null;
    File lockFile = null;
    boolean lockAcquired = false;
    try {
      /*
      * Acquire the file lock
      */
      lockFile = new File(inRoot, "lock");
      raFile = new RandomAccessFile(lockFile, "rw");
      fileChannel = raFile.getChannel();
      fileLock = fileChannel.tryLock();
      lockAcquired = true;
      solrClient.connect();
      Iterator<ExternalFile> iterator = iterate(inRoot);
      while (iterator.hasNext()) {
        ExternalFile externalFile = iterator.next();
        process(externalFile, outRoot, solrClient, mainCollection);
      }
    } finally {

      /*
      * Release the file lock
      */

      if(lockAcquired) {
        fileLock.release();
      }

      fileChannel.close();
      raFile.close();

      if(lockAcquired) {
        lockFile.delete();
        solrClient.close();
      }
    }
  }

  public static int hashCode(byte[] bytes, int offset, int length) {
    return Hash.murmurhash3_x86_32(bytes, offset, length, HASH_SEED);
  }

  public static Iterator<ExternalFile> iterate(String root) {
    return ExternalFile.iterate(root);
  }

  public static void process(ExternalFile externalFile, String outRoot, CloudSolrClient cloudSolrClient, String mainCollection) throws IOException {

    System.out.println("Processing External File:" + externalFile.file);

    if (!refresh(externalFile, outRoot)) {
      System.out.println("Old Data External File:" + externalFile.file);
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
      while ((line = in.readLine()) != null) {
        String[] pair = line.split(":");
        String id = pair[0].trim();
        float f = pair.length == 3 ? Float.parseFloat(pair[2].trim()) : Float.parseFloat(pair[1].trim());
        String rid = pair.length == 3 ? pair[1].trim() : id;

        Slice slice = docRouter.getTargetSlice(rid, null, null, null, docCollection);
        String shardId = slice.getName();
        if (shardOuts.containsKey(shardId)) {
          DataOutputStream shardOut = shardOuts.get(shardId);
          byte[] bytes = id.getBytes();
          shardOut.writeFloat(f);
          shardOut.writeByte(bytes.length);
          shardOut.write(bytes);
        } else {
          DataOutputStream shardOut = openShardOut(outRoot, externalFile, shardId, SHARD_TEMP_FILE);
          shardHomes.add(getShardHome(outRoot, externalFile, shardId));
          byte[] bytes = id.getBytes();
          shardOut.writeFloat(f);
          shardOut.writeByte(bytes.length);
          shardOut.write(bytes);
          shardOuts.put(shardId, shardOut);
        }
      }
    } finally {
      in.close();
      for (DataOutputStream dataOutputStream : shardOuts.values()) {
        dataOutputStream.close();
      }
    }
    long start = System.nanoTime();

    partitionShards(shardHomes);
    sortPartitions(shardHomes);
    long end = System.nanoTime();
    System.out.println("Time:" + (end - start) / (1_000_000));
  }


  public static void sortPartitions(List<File> shardHomes) throws IOException {
    for (File shardHome : shardHomes) {
      for (int i = 0; i < NUM_PARTITIONS; i++) {
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
    System.out.println("Sorting:"+partitionFile.getName());

    try {
      LinkedList<File> segments = new LinkedList<>();

      if (!partitionFile.exists()) {
        return segments;
      }

      partitionIn = new DataInputStream(new BufferedInputStream(new FileInputStream(partitionFile)));
      int segment = 0;
      boolean finished = false;
      while (!finished) {
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

        if (records.size() > 0) {
          Collections.sort(records, byteComp);
          File segmentFile = new File(shardHome, SEGMENT_TEMP_FILE + "." + partitionNumber + "." + segment);
          segments.addLast(segmentFile);
          writeSortedSegment(segmentFile, records);
          records.clear();
          ++segment;
        }
      }

      File finalSegment = mergeSegments(segments);
      finalSegment.renameTo(new File(finalSegment.getParentFile(), FINAL_PARTITION_PREFIX + partitionNumber));
      return segments;
    } finally {
      partitionFile.delete();
    }
  }

  public static void writeSortedSegment(File segmentFile, List<Record> records) throws IOException {

    DataOutputStream outSegment = null;

    try {
      outSegment = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(segmentFile)));
      for (Record record : records) {
        outSegment.writeFloat(record.f);
        outSegment.writeByte(record.length);
        outSegment.write(record.bytes, 0, record.length);
      }
    } finally {
      outSegment.close();
    }
  }

  public static File mergeSegments(LinkedList<File> segments) throws IOException {
    // Merge is a circular motion until there is only one segment.
    int mergeCount = 0;
    while (segments.size() > 1) {
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

    final byte[] file1Bytes = new byte[127];
    final byte[] file2Bytes = new byte[127];

    DataInputStream file1In = null;
    DataInputStream file2In = null;
    DataOutputStream mergeOut = null;
    File mergeFile = new File(file1.getParentFile(), MERGE_FILE_PREFIX + Integer.toString(mergeCount));

    try {

      mergeOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(mergeFile)));
      file1In = new DataInputStream(new BufferedInputStream(new FileInputStream(file1), 50000));
      file2In = new DataInputStream(new BufferedInputStream(new FileInputStream(file2), 50000));

      float f1 = file1In.readFloat();
      byte length1 = file1In.readByte();
      file1In.read(file1Bytes, 0, length1);

      float f2 = file2In.readFloat();
      byte length2 = file2In.readByte();
      file2In.read(file2Bytes, 0, length2);

      boolean file1Read = true;
      boolean file2Read = true;

      while (file1Read && file2Read) {
        int value = compare(file1Bytes, 0, length1, file2Bytes, 0, length2);

        if (value == 0) {

          // We are equal write both
          // In the case of unique ID's this should not happen
          mergeOut.writeFloat(f1);
          mergeOut.writeByte(length1);
          mergeOut.write(file1Bytes, 0, length1);

          mergeOut.writeFloat(f2);
          mergeOut.writeByte(length2);
          mergeOut.write(file2Bytes, 0, length2);

          //Advance left
          try {
            f1 = file1In.readFloat();
            length1 = file1In.readByte();
            file1In.read(file1Bytes, 0, length1);
          } catch (EOFException eof) {
            file1Read = false;
          }
          //Advance right
          try {
            f2 = file2In.readFloat();
            length2 = file2In.readByte();
            file2In.read(file2Bytes, 0, length2);
          } catch (EOFException eof) {
            file2Read = false;
          }

        } else if (value < 0) {
          // Write and advance file1
          mergeOut.writeFloat(f1);
          mergeOut.writeByte(length1);
          mergeOut.write(file1Bytes, 0, length1);

          try {
            f1 = file1In.readFloat();
            length1 = file1In.readByte();
            file1In.read(file1Bytes, 0, length1);
          } catch (EOFException eof) {
            file1Read = false;
          }
        } else {
          // Write and advance file2
          mergeOut.writeFloat(f2);
          mergeOut.writeByte(length2);
          mergeOut.write(file2Bytes, 0, length2);

          try {
            f2 = file2In.readFloat();
            length2 = file2In.readByte();
            file2In.read(file2Bytes, 0, length1);
          } catch (EOFException eof) {
            file2Read = false;
          }
        }
      }

      //One of the files is done write out the other file
      if (file1Read) {
        while (true) {

          mergeOut.writeFloat(f1);
          mergeOut.writeByte(length1);
          mergeOut.write(file1Bytes, 0, length1);

          try {

            f1 = file1In.readFloat();
            length1 = file1In.readByte();
            file1In.read(file1Bytes, 0, length1);

          } catch (EOFException eof) {
            // Do nothing
            break;
          }
        }
      }

      if (file2Read) {
        while (true) {
          mergeOut.writeFloat(f2);
          mergeOut.writeByte(length2);
          mergeOut.write(file2Bytes, 0, length2);

          try {
            f2 = file2In.readFloat();
            length2 = file2In.readByte();
            file2In.read(file2Bytes, 0, length1);
          } catch (EOFException eof) {
            // Do nothing
            break;
          }
        }
      }
    } finally {
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
      f = dataInputStream.readFloat();
      length = dataInputStream.readByte();
      bytes = new byte[length];
      dataInputStream.read(bytes, 0, length);
    }
  }

  public static class ByteComp implements Comparator<Record> {

    public int compare(Record rec1, Record rec2) {
      return ExternalFileUtil.compare(rec1.bytes, 0, rec1.length, rec2.bytes, 0, rec2.length);
    }
  }

  /*
  * Performs unsigned byte lexical comparison
   */
  public static final int compare(final byte[] left,
                                  final int leftOffset,
                                  final int length1,
                                  final byte[] right,
                                  final int rightOffset,
                                  final int length2) {

    final int minLength = length1 < length2 ? length1 : length2;

    int a;
    int b;

    for (int i = 0; i < minLength; ++i) {
      a = left[leftOffset+i] & 0xff;
      b = right[rightOffset+i] & 0xff;
      if (a != b) {
        return a - b;
      }
    }

    return length1 - length2;
  }

  public static void partitionShards(List<File> shardHomes) throws IOException {
    //Process the shard files
    byte[] bytes = new byte[128];

    for (File shardHome : shardHomes) {
      DataOutputStream[] partitions = new DataOutputStream[NUM_PARTITIONS];
      DataInputStream tempStream = null;
      File tempFile = new File(shardHome, SHARD_TEMP_FILE);
      try {
        tempStream = new DataInputStream(new BufferedInputStream(new FileInputStream(tempFile)));
        while (true) {
          float f = tempStream.readFloat();
          byte b = tempStream.readByte();
          tempStream.read(bytes, 0, b);
          int hash = hashCode(bytes, 0, b);
          int bucket = Math.abs(hash % partitions.length);
          if (partitions[bucket] == null) {
            partitions[bucket] = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(shardHome, SHARD_TEMP_FILE + "." + bucket))));
          }
          partitions[bucket].writeFloat(f);
          partitions[bucket].writeByte(b);
          partitions[bucket].write(bytes, 0, b);
        }
      } catch (EOFException e) {
        //File ended do nothing.
      } finally {
        tempStream.close();
        for (DataOutputStream dataOutputStream : partitions) {
          if (dataOutputStream != null) {
            dataOutputStream.close();
          }
        }
        tempFile.delete();
      }
    }
  }

  public static boolean refresh(ExternalFile externalFile, String outRoot) {
    File cdir = getFileNameOutDir(outRoot, externalFile);

    if (!cdir.exists()) {
      cdir.mkdirs();
      return true;
    }

    String[] dirs = cdir.list();
    for (String dir : dirs) {
      long ldir = Long.parseLong(dir);
      if (externalFile.timeStamp > ldir) {
        return true;
      }
    }
    return false;
  }


  public static String getHashDir(String fileName) {
    int bucket = fileName.hashCode() % HASH_DIRS;
    return "bucket" + bucket;
  }

  public static File getFileNameOutDir(String outRoot, ExternalFile externalFile) {
    return new File(new File(new File(outRoot), getHashDir(externalFile.fileName)), externalFile.fileName);
  }

  public static DataOutputStream openShardOut(String outRoot, ExternalFile externalFile, String shardId, String fileName) throws IOException {
    return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new File(getShardHome(outRoot, externalFile, shardId), fileName))));
  }

  public static File getShardHome(String outRoot, ExternalFile externalFile, String shardId) {
    File custDir = getFileNameOutDir(outRoot, externalFile);
    File file = new File(new File(custDir, Long.toString(externalFile.timeStamp)), shardId);
    if (!file.exists()) {
      file.mkdirs();
    }
    return file;
  }
}