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

package org.opensearch.index.fielddata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.cache.Cache;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.index.fielddata.ordinals.GlobalOrdinalsIndexFieldData;
import org.opensearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.opensearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.test.FieldMaskingReader;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class FieldDataCacheTests extends OpenSearchTestCase {

    public void testReaderCloseInvalidatesGlobalOrdinalsRetainingUnaccountedLeafData() throws Exception {
        AtomicLong accountedBytes = new AtomicLong();
        AtomicInteger sizeEvictions = new AtomicInteger();
        IndexFieldDataCache.Listener accountingListener = new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
                accountedBytes.addAndGet(ramUsage.ramBytesUsed());
            }

            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
                accountedBytes.addAndGet(-sizeInBytes);
                if (wasEvicted) {
                    sizeEvictions.incrementAndGet();
                }
            }
        };

        IndicesFieldDataCache nodeCache = new IndicesFieldDataCache(Settings.EMPTY, accountingListener, null, null);
        Cache<IndicesFieldDataCache.Key, Accountable> cache = nodeCache.getCache();

        try (Directory directory = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig(null).setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(directory, config)) {
                for (int segment = 0; segment < 2; segment++) {
                    for (int document = 0; document < 128; document++) {
                        Document doc = new Document();
                        doc.add(new StringField("field", "value_" + segment + "_" + document, Field.Store.NO));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
            }

            try (
                DirectoryReader reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(directory), new ShardId("index", "_na_", 0))
            ) {
                IndexFieldDataCache fieldCache = nodeCache.buildIndexFieldDataCache(new IndexFieldDataCache.Listener() {
                }, new Index("index", "_na_"), "field");
                PagedBytesIndexFieldData fieldData = createPagedBytes("field", fieldCache);
                fieldData.loadGlobal(reader);

                // Two leaf entries and one global-ordinals entry are initially accounted.
                assertEquals(3, cache.count());
                assertEquals(cache.weight(), accountedBytes.get());

                List<IndicesFieldDataCache.Key> leafKeys = new ArrayList<>();
                Iterator<IndicesFieldDataCache.Key> keys = cache.keys().iterator();
                Iterator<Accountable> values = cache.values().iterator();
                while (keys.hasNext() && values.hasNext()) {
                    IndicesFieldDataCache.Key key = keys.next();
                    if (values.next() instanceof LeafFieldData) {
                        leafKeys.add(key);
                    }
                }
                assertFalse(keys.hasNext());
                assertFalse(values.hasNext());
                assertEquals(2, leafKeys.size());
                leafKeys.forEach(cache::invalidate);

                // The cache and its listener now account only for the global ordinal map.
                assertEquals(1, cache.count());
                long globalBytes = cache.weight();
                assertEquals(globalBytes, accountedBytes.get());

                // Despite the leaf entries being gone, the cached global entry can still serve
                // their fielddata through its segmentAfd references.
                long retainedLeafBytes = retainedLeafBytes(cache, reader);
                assertTrue(retainedLeafBytes > 1);
                long maximumWeight = globalBytes + retainedLeafBytes / 2;
                assertTrue(globalBytes + retainedLeafBytes > maximumWeight);

                cache.setMaximumWeight(maximumWeight);
                cache.refresh();
                assertEquals(1, cache.count());
                assertEquals(0, sizeEvictions.get());

                // Simulate Cache.keys() omitting this entry during its one reader-close cleanup
                // pass. The old implementation consumed the close marker before this scan and
                // therefore could not retry the missed entry on the next pass.
                IndicesFieldDataCache nodeCacheSpy = spy(nodeCache);
                Cache<IndicesFieldDataCache.Key, Accountable> cacheSpy = spy(cache);
                doReturn(cacheSpy).when(nodeCacheSpy).getCache();
                doReturn(Collections.<IndicesFieldDataCache.Key>emptyList()).when(cacheSpy).keys();
                reader.close();
                nodeCacheSpy.clear();
                doCallRealMethod().when(cacheSpy).keys();
                nodeCacheSpy.clear();

                // Exact-key reader-close invalidation removes the retaining entry without needing
                // size pressure or a successful cache scan.
                assertEquals(0, cache.count());
                assertEquals(0, accountedBytes.get());
                assertEquals(0, sizeEvictions.get());
            }
        } finally {
            nodeCache.close();
        }
    }

    public void testLoadGlobal_neverCacheIfFieldIsMissing() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter iw = new IndexWriter(dir, iwc);
        long numDocs = scaledRandomIntBetween(32, 128);

        for (int i = 1; i <= numDocs; i++) {
            Document doc = new Document();
            doc.add(new SortedSetDocValuesField("field1", new BytesRef(String.valueOf(i))));
            doc.add(new StringField("field2", String.valueOf(i), Field.Store.NO));
            iw.addDocument(doc);
            if (i % 24 == 0) {
                iw.commit();
            }
        }
        iw.close();
        DirectoryReader ir = OpenSearchDirectoryReader.wrap(DirectoryReader.open(dir), new ShardId("_index", "_na_", 0));

        DummyAccountingFieldDataCache fieldDataCache = new DummyAccountingFieldDataCache();
        // Testing SortedSetOrdinalsIndexFieldData:
        SortedSetOrdinalsIndexFieldData sortedSetOrdinalsIndexFieldData = createSortedDV("field1", fieldDataCache);
        sortedSetOrdinalsIndexFieldData.loadGlobal(ir);
        assertThat(fieldDataCache.cachedGlobally, equalTo(1));
        sortedSetOrdinalsIndexFieldData.loadGlobal(new FieldMaskingReader("field1", ir));
        assertThat(fieldDataCache.cachedGlobally, equalTo(1));

        // Testing PagedBytesIndexFieldData
        PagedBytesIndexFieldData pagedBytesIndexFieldData = createPagedBytes("field2", fieldDataCache);
        pagedBytesIndexFieldData.loadGlobal(ir);
        assertThat(fieldDataCache.cachedGlobally, equalTo(2));
        pagedBytesIndexFieldData.loadGlobal(new FieldMaskingReader("field2", ir));
        assertThat(fieldDataCache.cachedGlobally, equalTo(2));

        ir.close();
        dir.close();
    }

    private SortedSetOrdinalsIndexFieldData createSortedDV(String fieldName, IndexFieldDataCache indexFieldDataCache) {
        return new SortedSetOrdinalsIndexFieldData(
            indexFieldDataCache,
            fieldName,
            CoreValuesSourceType.BYTES,
            new NoneCircuitBreakerService(),
            AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION
        );
    }

    private PagedBytesIndexFieldData createPagedBytes(String fieldName, IndexFieldDataCache indexFieldDataCache) {
        return new PagedBytesIndexFieldData(
            fieldName,
            CoreValuesSourceType.BYTES,
            indexFieldDataCache,
            new NoneCircuitBreakerService(),
            TextFieldMapper.Defaults.FIELDDATA_MIN_FREQUENCY,
            TextFieldMapper.Defaults.FIELDDATA_MAX_FREQUENCY,
            TextFieldMapper.Defaults.FIELDDATA_MIN_SEGMENT_SIZE
        );
    }

    private static long retainedLeafBytes(Cache<IndicesFieldDataCache.Key, Accountable> cache, DirectoryReader reader) throws Exception {
        GlobalOrdinalsIndexFieldData cachedGlobalOrdinals = null;
        for (Accountable value : cache.values()) {
            if (value instanceof GlobalOrdinalsIndexFieldData) {
                cachedGlobalOrdinals = (GlobalOrdinalsIndexFieldData) value;
                break;
            }
        }
        assertNotNull(cachedGlobalOrdinals);

        IndexOrdinalsFieldData consumer = cachedGlobalOrdinals.newConsumer(reader);
        long retainedBytes = 0;
        for (LeafReaderContext context : reader.leaves()) {
            retainedBytes += consumer.load(context).ramBytesUsed();
        }
        return retainedBytes;
    }

    // The cache captures shardIdentity at load time and skips the per-shard listener's onRemoval
    // when the current resolved identity differs (i.e. shard has been reallocated). The node-level
    // listener must still fire so node-wide accounting (e.g. circuit breaker) stays consistent.
    public void testCacheSkipsStalePerShardRemovalAfterReallocation() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter iw = new IndexWriter(dir, iwc);
        // Two segments so loadGlobal goes through the cache path (single-segment is short-circuited).
        for (int i = 0; i < 2; i++) {
            Document doc = new Document();
            doc.add(new SortedSetDocValuesField("field1", new BytesRef("v" + i)));
            iw.addDocument(doc);
            iw.commit();
        }
        iw.close();
        DirectoryReader ir = OpenSearchDirectoryReader.wrap(DirectoryReader.open(dir), new ShardId("idx", "_na_", 0));

        AtomicInteger nodeOnCache = new AtomicInteger();
        AtomicInteger nodeOnRemoval = new AtomicInteger();
        IndexFieldDataCache.Listener nodeListener = new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
                nodeOnCache.incrementAndGet();
            }

            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
                nodeOnRemoval.incrementAndGet();
            }
        };

        AtomicInteger perShardOnCache = new AtomicInteger();
        AtomicInteger perShardOnRemoval = new AtomicInteger();
        IndexFieldDataCache.Listener perShardListener = new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
                perShardOnCache.incrementAndGet();
            }

            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
                perShardOnRemoval.incrementAndGet();
            }
        };

        IndicesFieldDataCache nodeCache = new IndicesFieldDataCache(Settings.EMPTY, nodeListener, null, null);
        Index index = new Index("idx", "_na_");
        AtomicInteger currentIdentity = new AtomicInteger(4242);
        IndexFieldDataCache fieldCache = nodeCache.buildIndexFieldDataCache(
            perShardListener,
            index,
            "field1",
            shardId -> currentIdentity.get()
        );

        SortedSetOrdinalsIndexFieldData ifd = createSortedDV("field1", fieldCache);
        ifd.loadGlobal(ir);
        assertEquals(1, nodeOnCache.get());
        assertEquals(1, perShardOnCache.get());

        // Simulate shard reallocation: the resolver now returns a different identity. The cached
        // entry's captured identity (4242) no longer matches → per-shard onRemoval is skipped,
        // but the node-level onRemoval still fires.
        currentIdentity.set(9999);
        nodeCache.getCache().invalidateAll();

        assertEquals(1, nodeOnRemoval.get());
        assertEquals(0, perShardOnRemoval.get());

        ir.close();
        dir.close();
        nodeCache.close();
    }

    // When the resolved identity matches the captured identity, the per-shard listener's onRemoval
    // fires as normal.
    public void testCacheFiresPerShardRemovalWhenIdentityUnchanged() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter iw = new IndexWriter(dir, iwc);
        for (int i = 0; i < 2; i++) {
            Document doc = new Document();
            doc.add(new SortedSetDocValuesField("field1", new BytesRef("v" + i)));
            iw.addDocument(doc);
            iw.commit();
        }
        iw.close();
        DirectoryReader ir = OpenSearchDirectoryReader.wrap(DirectoryReader.open(dir), new ShardId("idx", "_na_", 0));

        AtomicInteger perShardOnRemoval = new AtomicInteger();
        IndexFieldDataCache.Listener perShardListener = new IndexFieldDataCache.Listener() {
            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
                perShardOnRemoval.incrementAndGet();
            }
        };

        IndicesFieldDataCache nodeCache = new IndicesFieldDataCache(Settings.EMPTY, new IndexFieldDataCache.Listener() {
        }, null, null);
        Index index = new Index("idx", "_na_");
        IndexFieldDataCache fieldCache = nodeCache.buildIndexFieldDataCache(perShardListener, index, "field1", shardId -> 4242);

        SortedSetOrdinalsIndexFieldData ifd = createSortedDV("field1", fieldCache);
        ifd.loadGlobal(ir);
        nodeCache.getCache().invalidateAll();

        assertEquals(1, perShardOnRemoval.get());

        ir.close();
        dir.close();
        nodeCache.close();
    }

    // When listeners throw from onCache/onRemoval, the cache must swallow and log so other
    // listeners and the load itself proceed normally.
    public void testListenerExceptionsAreSwallowedDuringCacheAndRemoval() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter iw = new IndexWriter(dir, iwc);
        for (int i = 0; i < 2; i++) {
            Document doc = new Document();
            doc.add(new SortedSetDocValuesField("field1", new BytesRef("v" + i)));
            iw.addDocument(doc);
            iw.commit();
        }
        iw.close();
        DirectoryReader ir = OpenSearchDirectoryReader.wrap(DirectoryReader.open(dir), new ShardId("idx", "_na_", 0));

        IndexFieldDataCache.Listener throwingNode = new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
                throw new RuntimeException("boom-cache-node");
            }

            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
                throw new RuntimeException("boom-removal-node");
            }
        };
        IndexFieldDataCache.Listener throwingPerShard = new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
                throw new RuntimeException("boom-cache-shard");
            }

            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
                throw new RuntimeException("boom-removal-shard");
            }
        };

        IndicesFieldDataCache nodeCache = new IndicesFieldDataCache(Settings.EMPTY, throwingNode, null, null);
        Index index = new Index("idx", "_na_");
        IndexFieldDataCache fieldCache = nodeCache.buildIndexFieldDataCache(throwingPerShard, index, "field1", shardId -> 1);

        SortedSetOrdinalsIndexFieldData ifd = createSortedDV("field1", fieldCache);
        // load should succeed despite throwing onCache listeners
        ifd.loadGlobal(ir);
        // removal path should also tolerate throwing listeners
        nodeCache.getCache().invalidateAll();

        ir.close();
        dir.close();
        nodeCache.close();
    }

    // The 3-arg buildIndexFieldDataCache overload uses a default resolver that returns
    // NO_SHARD_IDENTITY, which means the staleness check always allows per-shard listeners through.
    public void testThreeArgBuildUsesNoShardIdentitySentinel() throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig(null);
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        IndexWriter iw = new IndexWriter(dir, iwc);
        for (int i = 0; i < 2; i++) {
            Document doc = new Document();
            doc.add(new SortedSetDocValuesField("field1", new BytesRef("v" + i)));
            iw.addDocument(doc);
            iw.commit();
        }
        iw.close();
        DirectoryReader ir = OpenSearchDirectoryReader.wrap(DirectoryReader.open(dir), new ShardId("idx", "_na_", 0));

        AtomicInteger perShardOnRemoval = new AtomicInteger();
        IndexFieldDataCache.Listener perShardListener = new IndexFieldDataCache.Listener() {
            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
                perShardOnRemoval.incrementAndGet();
            }
        };

        IndicesFieldDataCache nodeCache = new IndicesFieldDataCache(Settings.EMPTY, new IndexFieldDataCache.Listener() {
        }, null, null);
        Index index = new Index("idx", "_na_");
        // 3-arg overload — no shardIdentityResolver supplied
        IndexFieldDataCache fieldCache = nodeCache.buildIndexFieldDataCache(perShardListener, index, "field1");

        SortedSetOrdinalsIndexFieldData ifd = createSortedDV("field1", fieldCache);
        ifd.loadGlobal(ir);
        nodeCache.getCache().invalidateAll();

        // Without identity tracking, every removal reaches the per-shard listener.
        assertEquals(1, perShardOnRemoval.get());

        ir.close();
        dir.close();
        nodeCache.close();
    }

    private class DummyAccountingFieldDataCache implements IndexFieldDataCache {

        private int cachedGlobally = 0;

        @Override
        public <FD extends LeafFieldData, IFD extends IndexFieldData<FD>> FD load(LeafReaderContext context, IFD indexFieldData)
            throws Exception {
            return indexFieldData.loadDirect(context);
        }

        @Override
        public <FD extends LeafFieldData, IFD extends IndexFieldData.Global<FD>> IFD load(DirectoryReader indexReader, IFD indexFieldData)
            throws Exception {
            cachedGlobally++;
            return (IFD) indexFieldData.loadGlobalDirect(indexReader);
        }

        @Override
        public void clear() {}

        @Override
        public void clear(String fieldName) {}
    }

}
