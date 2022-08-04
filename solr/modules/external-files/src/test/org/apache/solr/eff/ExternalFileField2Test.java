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

package org.apache.solr.eff;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.FileUtils;
import org.apache.solr.util.RefCounted;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@SolrTestCaseJ4.SuppressSSL
public class ExternalFileField2Test extends SolrTestCaseJ4 {
    @BeforeClass
    public static void beforeClass() throws Exception {
        initCore("solrconfig.xml", "schema.xml", getFile("external-files/solr").getAbsolutePath());
        updateExternalFile();
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        clearIndex();
        for (int i = 1; i <= 10; i++) {
            String id = Integer.toString(i);
            assertU("add a test doc", adoc("id", id));
        }
        assertU(commit());
    }

    static void updateExternalFile() throws IOException {
        final String testHome = SolrTestCaseJ4.getFile("external-files").getParent();
        String filename = "external_eff";
        FileUtils.copyFile(new File(testHome + "/" + filename),
                new File(h.getCore().getDataDir() + "/external_dyna_eff_A"));
        FileUtils.copyFile(new File(testHome + "/" + filename),
                new File(h.getCore().getDataDir() + "/external_dyna_eff_B"));
    }

    public void testDyanmicEFFCache() throws Exception {
        adoc("id", "2112");
        req(commit("openSearcher", "true"));  // reset caches

        h.getCore().withSearcher(searcher -> {
            SolrCache<?, ?> cache = searcher.getCache("fileFloatSourceCache_eff");
            assertEquals(0, cache.size());
            return null;
        });

        // Make 5 requests, for 5 different EFF's, `dyna_eff_0` ... `dyna_eff_4`
        for (int i=0; i < 5; i++) {
            assertQ("query",
                    req("q", "*:*", "rows", "1", "fl", "id,eff_val:field(dyna_eff_" + i + ")"),
                    "//result/doc/str[@name='id']",
                    "//result/doc/float[@name='eff_val']",
                    "//result/doc/float[@name='eff_val'][.='99.99']"
            );
        }

        h.getCore().withSearcher(searcher -> {
            SolrCache<?, ?> cache = searcher.getCache("fileFloatSourceCache_eff");
            assertEquals(2, cache.size());
            Map<String, Object> metrics = cache.getSolrMetricsContext().getMetricsSnapshot();
            assertEquals(5L, metrics.get("CACHE.searcher.fileFloatSourceCache_eff.inserts"));
            assertEquals(3L, metrics.get("CACHE.searcher.fileFloatSourceCache_eff.evictions"));
            return null;
        });
    }

    public void testDynamicEFFDefault() {
        // Ask for `dyna_eff_NODATA` (matching dynamicField `dyna_eff_*`) but there are no such
        // external_dyna_eff_NODATA* files, so default value (99.99, from schema-eff.xml) is returned.
        assertQ("query",
                req("q", "*:*", "rows", "1", "fl", "id,eff_val:field(dyna_eff_NODATA)"),
                "//result/doc/str[@name='id']",
                "//result/doc/float[@name='eff_val']",
                "//result/doc/float[@name='eff_val'][.='99.99']"
        );
    }

    public void testDynamicEFFValues() {
        assertQ("query",
                req("q", "id:1", "rows", "1", "fl", "id,eff_val:field(dyna_eff_A)"),
                "//result/doc/str[@name='id']",
                "//result/doc/float[@name='eff_val']",
                "//result/doc/float[@name='eff_val'][.='0.354']"
        );
        assertQ("query",
                req("q", "id:7", "rows", "1", "fl", "id,eff_val:field(dyna_eff_B)"),
                "//result/doc/str[@name='id']",
                "//result/doc/float[@name='eff_val']",
                "//result/doc/float[@name='eff_val'][.='3.957']"
        );
    }

    public void testRegenerator() throws Exception {
        h.getCore().withSearcher(searcher -> {
            SolrCache<?,?> cache = searcher.getCache("fileFloatSourceCache_effWarmed");
            assertEquals(0, cache.size());
            return null;
        });

        assertQ("query",
                req("q", "id:7", "rows", "1", "fl", "id,eff_val:field(effWarmed)"),
                "//result/doc/str[@name='id']",
                "//result/doc/float[@name='eff_val']",
                "//result/doc/float[@name='eff_val'][.='0.1']"
        );

        h.getCore().withSearcher(searcher -> {
            SolrCache<?,?> cache = searcher.getCache("fileFloatSourceCache_effWarmed");
            assertEquals(1, cache.size());
            return null;
        });

        assertU(adoc("id", "2112"));
        assertU(commit());  // reset caches
        assertQ(req("q", "id:2112"), "//*[@numFound='1']");

        waitForWarming();
        SolrIndexSearcher newestSearcher = h.getCore().getNewestSearcher(true).get();
        SolrCache<?,?> c = newestSearcher.getCache("fileFloatSourceCache_effWarmed");
        assertEquals(1, c.size());

        // `effWarmed` is configured to warm 1 item
        h.getCore().withSearcher(searcher -> {
            SolrCache<?,?> cache = searcher.getCache("fileFloatSourceCache_effWarmed");
            assertEquals(1, cache.size());
            return null;
        });
    }

}
