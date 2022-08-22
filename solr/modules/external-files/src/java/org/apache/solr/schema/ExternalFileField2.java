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

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.SortField;
import org.apache.solr.common.SolrException;
import org.apache.solr.request.SolrRequestInfo;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.QParser;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.search.function.FileFloatSource2;
import org.apache.solr.uninverting.UninvertingReader;
import org.apache.solr.util.RefCounted;

import java.io.IOException;
import java.util.Map;

public class ExternalFileField2 extends FieldType implements SchemaAware {

  private static final String EXTERNAL_ROOT_PATH_VAR = "EXTERNAL_ROOT_PATH";
  private String keyFieldName;
  private IndexSchema schema;
  private float defVal;

  @Override
  protected void init(IndexSchema schema, Map<String, String> args) {
    restrictProps(SORT_MISSING_FIRST | SORT_MISSING_LAST);
    keyFieldName = args.remove("keyField");
    String defValS = args.remove("defVal");
    defVal = defValS == null ? 0 : Float.parseFloat(defValS);
    this.schema = schema;
  }

  @Override
  public void write(TextResponseWriter writer, String name, IndexableField f) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public UninvertingReader.Type getUninversionType(SchemaField sf) {
    return null;
  }

  @Override
  public SortField getSortField(SchemaField field, boolean reverse) {
    FileFloatSource2 source = getFileFloatSource(field);
    return source.getSortField(reverse);
  }

  @Override
  public ValueSource getValueSource(SchemaField field, QParser parser) {
    return getFileFloatSource(field);
  }

  public FileFloatSource2 getFileFloatSource(SchemaField field, String datadir, String searcherId, String shardId) {
    // Because the float source uses a static cache, all source objects will
    // refer to the same data.
    return new FileFloatSource2(field, getKeyField(), defVal, datadir, getExternalRoot(), searcherId, shardId);
  }

  public FileFloatSource2 getFileFloatSource(SchemaField field) {
    RefCounted<SolrIndexSearcher> solrIndexSearcherRefCounted = SolrRequestInfo.getRequestInfo().getReq().getCore().getSearcher();
    String searcherId = null;
    try {
      searcherId = Integer.toHexString(solrIndexSearcherRefCounted.get().hashCode());
    } finally {
      solrIndexSearcherRefCounted.decref();
    }

    String shardId = SolrRequestInfo.getRequestInfo().getReq().getCore().getCoreDescriptor().getCloudDescriptor().getShardId();

    return getFileFloatSource(
        field, SolrRequestInfo.getRequestInfo().getReq().getCore().getDataDir(), searcherId, shardId) ;
  }

  // If no key field is defined, we use the unique key field
  private SchemaField getKeyField() {
    return keyFieldName == null ? schema.getUniqueKeyField() : schema.getField(keyFieldName);
  }

  @Override
  public void inform(IndexSchema schema) {
    this.schema = schema;

    if (keyFieldName != null && schema.getFieldType(keyFieldName).isPointField()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "keyField '" + keyFieldName + "' has a Point field type, which is not supported.");
    }
  }

  private String getExternalRoot() {
    String path =  null;
    //First check the env, than the system props.
    if ((path = System.getenv(EXTERNAL_ROOT_PATH_VAR)) != null) {
      return path;
    } else if ((path = System.getProperty(EXTERNAL_ROOT_PATH_VAR)) != null) {
      return path;
    }

    return null;
  }
}