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

import java.io.*;
import java.util.*;

public class ExternalFile {

  long timeStamp;
  String fileName;
  File file;

  public ExternalFile(File file, String fileName, long timeStamp) {
    this.file = file;
    this.timeStamp = timeStamp;
    this.fileName = fileName;
  }

  public static Iterator<ExternalFile> iterate(String root) {
    return new ExternalFileIterator(root);
  }

  public static class FileTimeComp implements Comparator<File> {
    public int compare(File f1, File f2) {
      long l1 = Long.parseLong(parseIncTime(f1.getName()));
      long l2 = Long.parseLong(parseIncTime(f2.getName()));
      return Long.compare(l1, l2);
    }
  }

  public static String parseIncTime(String s) {
    return s.split("\\.")[0].split("-")[1];
  }

  public static void sortIncrementals(File[] incremenantals) {
    Arrays.sort(incremenantals, new FileTimeComp());
  }

  public static class ExternalFileIterator implements Iterator<ExternalFile> {

    File[] topLevelDirs = null;
    int topLevelIndex = 0;
    File[] fileLevelDirs = null;
    int fileLevelIndex = 0;

    public ExternalFileIterator(String root) {
        File rootDir = new File(root);
        topLevelDirs = rootDir.listFiles(File::isDirectory);
        fileLevelDirs = topLevelDirs[0].listFiles();
    }

    public boolean hasNext() {
      if(topLevelIndex < topLevelDirs.length) {
        return true;
      } else {
        return false;
      }
    }

    public ExternalFile next() {
      while(true) {
        if (fileLevelIndex < fileLevelDirs.length) {
          File currentFile = fileLevelDirs[fileLevelIndex];
          ++fileLevelIndex;
          if (fileLevelIndex == fileLevelDirs.length) {
            ++topLevelIndex;
            if (topLevelIndex < topLevelDirs.length) {
              fileLevelDirs = topLevelDirs[topLevelIndex].listFiles();
              fileLevelIndex = 0;
            }
          }
          //Find the latest version of the file
          File[] timeStamps = currentFile.listFiles(File::isDirectory);
          File timeDir = null;
          long time = -1;
          for (File timeStamp : timeStamps) {
            try {
              long fileTime = Long.parseLong(timeStamp.getName());
              if (fileTime > time) {
                time = fileTime;
                timeDir = timeStamp;
              }
            } catch (Exception e) {
              //skip
            }
          }

          // Deal with the incrementals
          File[] incrementals = currentFile.listFiles(File::isFile);

          if (timeDir != null && incrementals != null && incrementals.length > 0) {
            sortIncrementals(incrementals);
            //File name should be file-timestamp.inc
            String lastInc = parseIncTime(incrementals[incrementals.length-1].getName());
            Map<String, String> imap = new HashMap<>();
            BufferedReader in = null;
            for (File incremental : incrementals) {
              long itime = Long.parseLong(parseIncTime(incremental.getName()));

              if(itime <= time) {
                // Skip incremental that have a timestamp less then or equal to the last full file.
                continue;
              }

              try {
                in = new BufferedReader(new FileReader(incremental));
                String line = null;
                while ((line = in.readLine()) != null) {
                  String[] pair = line.split(":", 2);
                  imap.put(pair[0], pair[1]);
                }
              } catch (Exception fe) {
                throw new RuntimeException(fe);
              } finally {
                try {
                  in.close();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            }

            if(imap.size() > 0) {
              // Create the new timeDir and write the new full external file with incrementals applied
              File incDir = new File(currentFile, lastInc);
              incDir.mkdirs();
              File newFile = new File(incDir, currentFile.getName()+".txt");

              File[] targets = timeDir.listFiles();
              File targetFile = null;
              if (targets.length > 0) {
                if (targets[0].getName().endsWith(".txt")) {
                  targetFile = targets[0];
                }
              }

              PrintWriter newOut = null;
              BufferedReader targetIn = null;
              try {
                targetIn = new BufferedReader(new FileReader(targetFile));
                newOut = new PrintWriter(new BufferedWriter(new FileWriter(newFile)));
                String line = null;
                while ((line = targetIn.readLine()) != null) {
                  String pair[] = line.split(":");
                  if(imap.containsKey(pair[0])) {
                    newOut.println(pair[0]+":"+imap.get(pair[0]));
                  } else {
                    newOut.println(line);
                  }
                }
                // Delete all the incrementals.
                for (File ifile : incrementals) {
                  ifile.delete();
                }
                // Reset the file pointer and time
                timeDir = incDir;
                time = Long.parseLong(timeDir.getName());
              } catch (Exception e) {
                throw new RuntimeException(e);
              } finally {
                try {
                  targetIn.close();
                  newOut.close();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            }
          }

          if (timeDir != null) {
            File[] targets = timeDir.listFiles();
            if (targets.length > 0) {
              if (targets[0].getName().endsWith(".txt")) {
                String fileName = currentFile.getName();
                return new ExternalFile(targets[0], fileName, time);
              }
            }
          }
        } else {
          return null;
        }
      }
    }
  }
}