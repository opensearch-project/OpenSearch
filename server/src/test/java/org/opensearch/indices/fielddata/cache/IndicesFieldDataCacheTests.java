/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices.fielddata.cache;

import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.Accountable;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collection;
import java.util.Collections;

import static org.opensearch.indices.fielddata.cache.IndicesFieldDataCache.INDICES_FIELDDATA_CACHE_SIZE_KEY;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IndicesFieldDataCacheTests extends OpenSearchTestCase {

    private static final IndexFieldDataCache.Listener NOOP_LISTENER = new IndexFieldDataCache.Listener() {
    };

    public void testUnboundedCacheCanBecomeBoundedAndUnboundedAgain() throws Exception {
        Settings settings = Settings.builder().put(INDICES_FIELDDATA_CACHE_SIZE_KEY.getKey(), "-1").build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);

        try (
            ByteBuffersDirectory directory = new ByteBuffersDirectory();
            IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(new KeywordAnalyzer()))
        ) {
            writer.addDocument(new Document());
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                IndicesFieldDataCache fieldDataCache = new IndicesFieldDataCache(settings, NOOP_LISTENER, clusterService, null);
                Cache<IndicesFieldDataCache.Key, Accountable> cache = fieldDataCache.getCache();
                IndexReader.CacheKey readerKey = reader.getReaderCacheHelper().getKey();

                assertEquals(-1, cache.getMaximumWeight());
                cache.computeIfAbsent(key(fieldDataCache, "field1", readerKey), ignored -> accountable(10));
                cache.computeIfAbsent(key(fieldDataCache, "field2", readerKey), ignored -> accountable(10));
                assertEquals(20, cache.weight());

                clusterSettings.applySettings(Settings.builder().put(INDICES_FIELDDATA_CACHE_SIZE_KEY.getKey(), "15b").build());
                assertEquals(15, cache.getMaximumWeight());
                assertTrue(cache.weight() <= 15);

                clusterSettings.applySettings(Settings.builder().put(INDICES_FIELDDATA_CACHE_SIZE_KEY.getKey(), "-1").build());
                assertEquals(-1, cache.getMaximumWeight());
                cache.computeIfAbsent(key(fieldDataCache, "field3", readerKey), ignored -> accountable(10));
                cache.computeIfAbsent(key(fieldDataCache, "field4", readerKey), ignored -> accountable(10));
                assertTrue(cache.weight() > 15);

                clusterSettings.applySettings(Settings.builder().put(INDICES_FIELDDATA_CACHE_SIZE_KEY.getKey(), "0").build());
                assertEquals(-1, cache.getMaximumWeight());
            }
        }
    }

    private IndicesFieldDataCache.Key key(IndicesFieldDataCache fieldDataCache, String fieldName, IndexReader.CacheKey readerKey) {
        IndicesFieldDataCache.IndexFieldCache indexFieldCache = new IndicesFieldDataCache.IndexFieldCache(
            logger,
            fieldDataCache,
            new Index("index", "uuid"),
            fieldName,
            shardId -> IndicesFieldDataCache.Key.NO_SHARD_IDENTITY,
            NOOP_LISTENER
        );
        return new IndicesFieldDataCache.Key(indexFieldCache, readerKey, null);
    }

    private static Accountable accountable(long ramBytesUsed) {
        return new Accountable() {
            @Override
            public long ramBytesUsed() {
                return ramBytesUsed;
            }

            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }
        };
    }
}
