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
  String customer;
  File file;

  public ExternalFile(File file, String customer, long timeStamp) {
    this.file = file;
    this.timeStamp = timeStamp;
    this.customer = customer;
  }

  public static Iterator<ExternalFile> iterate(String root) {
    return new ExternalFileIterator(root);
  }

  public static class ExternalFileIterator implements Iterator<ExternalFile> {

    String[] customers = null;
    int customerIndex = 0;


    public ExternalFileIterator(String root) {
        File customersRoot = new File(root);
        customers = customersRoot.list();
    }

    public boolean hasNext() {
      if(customerIndex == 0) {
        return true;
      }

      return false;
    }

    public ExternalFile next() {
      ++customerIndex;
      File file = new File("/Users/joelbernstein/tools/merge/test/customer1/pricing/1659368012/customer1.txt");
      return new ExternalFile(file, "customer1", 121213131);
    }
  }

}