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

import java.io.File;
import java.util.Iterator;

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

  public static class ExternalFileIterator implements Iterator<ExternalFile> {

    File[] topLevelDirs = null;
    int topLevelIndex = 0;
    File[] fileLevelDirs = null;
    int fileLevelIndex = 0;

    public ExternalFileIterator(String root) {
        File rootDir = new File(root);
        topLevelDirs = rootDir.listFiles();
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
          File[] timeStamps = currentFile.listFiles();
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