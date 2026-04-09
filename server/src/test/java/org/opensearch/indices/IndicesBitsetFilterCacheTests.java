/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.Accountable;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class IndicesBitsetFilterCacheTests extends OpenSearchTestCase {

    private ThreadPool threadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("indices_bitset_filter_cache_test");
    }

    @Override
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        super.tearDown();
    }

    private static final BitsetFilterCache.Listener NO_OP_LISTENER = new BitsetFilterCache.Listener() {
        @Override
        public void onCache(ShardId shardId, Accountable accountable) {}

        @Override
        public void onRemoval(ShardId shardId, Accountable accountable) {}
    };

    /**
     * Verifies that when the cache size setting is configured, the cache evicts entries
     * once the total weight exceeds the configured limit.
     */
    public void testCacheSizeLimitIsHonored() throws Exception {
        // First, figure out how large a single bitset entry is by caching one.
        long singleEntryBytes;
        try (IndicesBitsetFilterCache probeCache = new IndicesBitsetFilterCache(Settings.EMPTY, threadPool)) {
            IndexWriter writer = new IndexWriter(
                new ByteBuffersDirectory(),
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(new LogByteSizeMergePolicy())
            );
            Document doc = new Document();
            doc.add(new StringField("field", "value", Field.Store.NO));
            writer.addDocument(doc);
            writer.commit();

            DirectoryReader reader = DirectoryReader.open(writer);
            reader = OpenSearchDirectoryReader.wrap(reader, new ShardId("probe", "_na_", 0));

            BitSetProducer producer = probeCache.getBitSetProducer(new TermQuery(new Term("field", "value")), NO_OP_LISTENER);
            producer.getBitSet(reader.leaves().get(0));

            assertThat(probeCache.getCache().count(), equalTo(1));
            singleEntryBytes = probeCache.getCache().weight();
            assertThat(singleEntryBytes, greaterThan(0L));

            reader.close();
            writer.close();
        }

        // Now create a cache with a size limit that fits exactly 2 entries.
        long cacheSizeBytes = singleEntryBytes * 2;
        Settings settings = Settings.builder().put("indices.cache.bitset.size", cacheSizeBytes + "b").build();

        try (IndicesBitsetFilterCache cache = new IndicesBitsetFilterCache(settings, threadPool)) {
            // Create an index with 3 separate segments (3 commits), each with one doc matching a different query.
            IndexWriter writer = new IndexWriter(
                new ByteBuffersDirectory(),
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(new LogByteSizeMergePolicy())
            );

            for (int i = 0; i < 3; i++) {
                Document doc = new Document();
                doc.add(new StringField("field", "val" + i, Field.Store.NO));
                writer.addDocument(doc);
                writer.commit();
            }

            DirectoryReader reader = DirectoryReader.open(writer);
            reader = OpenSearchDirectoryReader.wrap(reader, new ShardId("test", "_na_", 0));

            // 3 segments, cache 3 different queries — one per segment.
            // Each segment has exactly 1 leaf.
            assertThat(reader.leaves().size(), equalTo(3));

            for (int i = 0; i < 3; i++) {
                LeafReaderContext leaf = reader.leaves().get(i);
                BitSetProducer producer = cache.getBitSetProducer(new TermQuery(new Term("field", "val" + i)), NO_OP_LISTENER);
                producer.getBitSet(leaf);
            }

            // We inserted 3 entries but the cache can only hold 2.
            // The LRU eviction should have kicked in.
            assertThat(cache.getCache().count(), lessThanOrEqualTo(2));
            assertThat(cache.getCache().weight(), lessThanOrEqualTo(cacheSizeBytes));

            reader.close();
            writer.close();
        }
    }

    /**
     * Verifies that the onRemoval listener is called when entries are evicted due to size limit.
     */
    public void testEvictionTriggersOnRemovalListener() throws Exception {
        // Probe for single entry size.
        long singleEntryBytes;
        try (IndicesBitsetFilterCache probeCache = new IndicesBitsetFilterCache(Settings.EMPTY, threadPool)) {
            IndexWriter writer = new IndexWriter(
                new ByteBuffersDirectory(),
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(new LogByteSizeMergePolicy())
            );
            Document doc = new Document();
            doc.add(new StringField("field", "value", Field.Store.NO));
            writer.addDocument(doc);
            writer.commit();

            DirectoryReader reader = DirectoryReader.open(writer);
            reader = OpenSearchDirectoryReader.wrap(reader, new ShardId("probe", "_na_", 0));

            probeCache.getBitSetProducer(new TermQuery(new Term("field", "value")), NO_OP_LISTENER).getBitSet(reader.leaves().get(0));
            singleEntryBytes = probeCache.getCache().weight();

            reader.close();
            writer.close();
        }

        // Cache fits only 1 entry.
        long cacheSizeBytes = singleEntryBytes;
        Settings settings = Settings.builder().put("indices.cache.bitset.size", cacheSizeBytes + "b").build();

        final AtomicLong removedBytes = new AtomicLong();
        BitsetFilterCache.Listener trackingListener = new BitsetFilterCache.Listener() {
            @Override
            public void onCache(ShardId shardId, Accountable accountable) {}

            @Override
            public void onRemoval(ShardId shardId, Accountable accountable) {
                if (accountable != null) {
                    removedBytes.addAndGet(accountable.ramBytesUsed());
                }
            }
        };

        try (IndicesBitsetFilterCache cache = new IndicesBitsetFilterCache(settings, threadPool)) {
            IndexWriter writer = new IndexWriter(
                new ByteBuffersDirectory(),
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(new LogByteSizeMergePolicy())
            );

            for (int i = 0; i < 2; i++) {
                Document doc = new Document();
                doc.add(new StringField("field", "val" + i, Field.Store.NO));
                writer.addDocument(doc);
                writer.commit();
            }

            DirectoryReader reader = DirectoryReader.open(writer);
            reader = OpenSearchDirectoryReader.wrap(reader, new ShardId("test", "_na_", 0));
            assertThat(reader.leaves().size(), equalTo(2));

            // Cache first entry.
            cache.getBitSetProducer(new TermQuery(new Term("field", "val0")), trackingListener).getBitSet(reader.leaves().get(0));
            assertThat(cache.getCache().count(), equalTo(1));
            assertThat(removedBytes.get(), equalTo(0L));

            // Cache second entry — should evict the first since limit is 1 entry.
            cache.getBitSetProducer(new TermQuery(new Term("field", "val1")), trackingListener).getBitSet(reader.leaves().get(1));
            assertThat(cache.getCache().count(), equalTo(1));
            assertThat(removedBytes.get(), greaterThan(0L));

            reader.close();
            writer.close();
        }
    }

    /**
     * Verifies that stale entries from closed readers are purged.
     */
    public void testStaleEntriesPurgedAfterReaderClose() throws Exception {
        try (IndicesBitsetFilterCache cache = new IndicesBitsetFilterCache(Settings.EMPTY, threadPool)) {
            IndexWriter writer = new IndexWriter(
                new ByteBuffersDirectory(),
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(new LogByteSizeMergePolicy())
            );

            Document doc = new Document();
            doc.add(new StringField("field", "value", Field.Store.NO));
            writer.addDocument(doc);
            writer.commit();

            DirectoryReader reader = DirectoryReader.open(writer);
            reader = OpenSearchDirectoryReader.wrap(reader, new ShardId("test", "_na_", 0));

            cache.getBitSetProducer(new TermQuery(new Term("field", "value")), NO_OP_LISTENER).getBitSet(reader.leaves().get(0));
            assertThat(cache.getCache().count(), equalTo(1));

            // Close writer first so no references remain, then close reader.
            writer.close();
            reader.close();

            // Purge stale entries.
            cache.purgeStaleEntries();
            assertThat(cache.getCache().count(), equalTo(0));
        }
    }

    /**
     * Verifies that entries from multiple indices share the same cache and the size limit applies globally.
     */
    public void testMultipleIndicesShareCacheWithGlobalSizeLimit() throws Exception {
        // Probe for single entry size.
        long singleEntryBytes;
        try (IndicesBitsetFilterCache probeCache = new IndicesBitsetFilterCache(Settings.EMPTY, threadPool)) {
            IndexWriter writer = new IndexWriter(
                new ByteBuffersDirectory(),
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(new LogByteSizeMergePolicy())
            );
            Document doc = new Document();
            doc.add(new StringField("field", "value", Field.Store.NO));
            writer.addDocument(doc);
            writer.commit();

            DirectoryReader reader = DirectoryReader.open(writer);
            reader = OpenSearchDirectoryReader.wrap(reader, new ShardId("probe", "_na_", 0));

            probeCache.getBitSetProducer(new TermQuery(new Term("field", "value")), NO_OP_LISTENER).getBitSet(reader.leaves().get(0));
            singleEntryBytes = probeCache.getCache().weight();

            reader.close();
            writer.close();
        }

        // Cache fits 2 entries total across all indices.
        long cacheSizeBytes = singleEntryBytes * 2;
        Settings settings = Settings.builder().put("indices.cache.bitset.size", cacheSizeBytes + "b").build();

        try (IndicesBitsetFilterCache cache = new IndicesBitsetFilterCache(settings, threadPool)) {
            // Create two separate "indices" (different ShardIds, different writers).
            IndexWriter writer1 = new IndexWriter(
                new ByteBuffersDirectory(),
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(new LogByteSizeMergePolicy())
            );
            Document doc1 = new Document();
            doc1.add(new StringField("field", "val1", Field.Store.NO));
            writer1.addDocument(doc1);
            writer1.commit();

            IndexWriter writer2 = new IndexWriter(
                new ByteBuffersDirectory(),
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(new LogByteSizeMergePolicy())
            );
            Document doc2 = new Document();
            doc2.add(new StringField("field", "val2", Field.Store.NO));
            writer2.addDocument(doc2);
            writer2.commit();

            IndexWriter writer3 = new IndexWriter(
                new ByteBuffersDirectory(),
                new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(new LogByteSizeMergePolicy())
            );
            Document doc3 = new Document();
            doc3.add(new StringField("field", "val3", Field.Store.NO));
            writer3.addDocument(doc3);
            writer3.commit();

            DirectoryReader reader1 = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer1), new ShardId("index1", "_na_", 0));
            DirectoryReader reader2 = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer2), new ShardId("index2", "_na_", 0));
            DirectoryReader reader3 = OpenSearchDirectoryReader.wrap(DirectoryReader.open(writer3), new ShardId("index3", "_na_", 0));

            // Cache one entry from each "index".
            cache.getBitSetProducer(new TermQuery(new Term("field", "val1")), NO_OP_LISTENER).getBitSet(reader1.leaves().get(0));
            cache.getBitSetProducer(new TermQuery(new Term("field", "val2")), NO_OP_LISTENER).getBitSet(reader2.leaves().get(0));
            cache.getBitSetProducer(new TermQuery(new Term("field", "val3")), NO_OP_LISTENER).getBitSet(reader3.leaves().get(0));

            // 3 entries inserted but only 2 fit — global eviction should have kicked in.
            assertThat(cache.getCache().count(), lessThanOrEqualTo(2));
            assertThat(cache.getCache().weight(), lessThanOrEqualTo(cacheSizeBytes));

            reader1.close();
            reader2.close();
            reader3.close();
            writer1.close();
            writer2.close();
            writer3.close();
        }
    }
}
