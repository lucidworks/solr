package org.apache.solr.eff;

import org.apache.solr.search.CacheRegenerator;
import org.apache.solr.search.SolrCache;
import org.apache.solr.search.SolrIndexSearcher;

import java.io.IOException;

@SuppressWarnings({"unchecked"})
public class ExternalFileFieldRegenerator implements CacheRegenerator {
    @Override
    public <K, V> boolean regenerateItem(SolrIndexSearcher newSearcher, SolrCache<K, V> newCache, SolrCache<K, V> oldCache, K oldKey, V oldVal) throws IOException {
        FileFloatSource2 ffs = ((FileFloatSource2.Entry) oldKey).ffs;
        newCache.put(oldKey, (V) FileFloatSource2.getFloats(ffs, newSearcher.getIndexReader()));
        return true;
    }
}
