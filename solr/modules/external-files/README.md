<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

ExternalFileField2
==================

Introduction
------------
A highly scalable implementation of externally provided `float` type data,
allowing for many keyed values for a single, dynamically mapped field. 

Example
-------

Run the following script to see ExternalFileField2 in action:

    # Start Solr with external-files module, create collection, index document, create external price data
    export COLLECTION=products
    bin/solr start -c -Dsolr.modules=external-files
    bin/solr create -c $COLLECTION
    bin/post -c $COLLECTION -type text/csv -out yes -d $'id,default_price_f\n1,42.42'
    echo "1=37.97" > server/solr/${COLLECTION}_shard1_replica_n1/data/external_price_00001

    # Add cache for `external_prices`    
    curl -X POST -H 'Content-type:application/json' -d '{
     "add-cache" : {
         name: "fileFloatSourceCache_external_prices",
         class: "solr.search.CaffeineCache",
         size: 100, 
         maxRamMB: 2048,
         initialSize:0, 
         autowarmCount:10,
         regenerator:"org.apache.solr.eff.ExternalFileFieldRegenerator"
     }
    }' http://localhost:8983/api/collections/$COLLECTION/config
        
    # Define `external_prices` field type
    curl -X POST -H 'Content-type:application/json' --data-binary '{
        "add-field-type" : {
            name: "external_prices",
            class: "org.apache.solr.eff.ExternalFileField2",
            keyField: "id", 
            defVal: "0.00",
            stored: "false",
            indexed: "false"
        }
    }' http://localhost:8983/api/collections/$COLLECTION/schema

    # define `price_*` dynamic field mapping to `external_prices`
    curl -X POST -H 'Content-type:application/json' --data-binary '{
      "add-dynamic-field":{
         "name":"price_*",
         "type":"external_prices",
         "stored": "false" }
    }' http://localhost:8983/api/collections/$COLLECTION/schema


    # Search for products within the context of store `00001`, returning, and sorting by computed price:
    curl "http://localhost:8983/solr/$COLLECTION/select?q=*:*&store=00001&price_field=price_$\{store\}&computed_price=if(eq(field($\{price_field\}),0.00),default_price_f,field($\{price_field\}))&fl=id,default_price_f,computed_price:$\{computed_price\}&sort=$\{computed_price\}+asc"

    # Fetch cache stats available here:
    curl "http://localhost:8983/solr/admin/metrics?m=solr.core.$COLLECTION.shard1.replica_n1:CACHE.searcher.fileFloatSourceCache_external_prices&key=$\{m\}:size&key=$\{m\}:lookups&key=$\{m\}:hits&key=$\{m\}:hitratio&key=$\{m\}:inserts&key=$\{m\}:evictions&key=$\{m\}:warmupTime&key=$\{m\}:ramBytesUsed"  
