/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.fielddata.cache;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Accountable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache.IndexFieldCache;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache.Key;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.atomic.AtomicReference;

public class IndicesFieldDataCacheTests extends OpenSearchTestCase {

    /**
     * Reproduces the fielddata leak from the clear-index path (clear-cache API, fielddata settings
     * change, index close): an entry marked for cleanup is silently skipped when a concurrent cache
     * hit promotes it to the head of the LRU list after the sweep's cursor has already passed the
     * head. Because {@link IndicesFieldDataCache#clear()} consumes the marks before scanning, the
     * skipped entry is never revisited and leaks. The promotion is triggered deterministically from
     * the removal listener of the first swept entry, mimicking a search touching the cache while the
     * cleaner runs. This test fails if the sweep iterates the live {@code Cache#keys()} LRU view and
     * passes with the point-in-time {@code Cache#keysSnapshot()}.
     */
    public void testClearIndexIsNotDefeatedByLruPromotionMidSweep() throws Exception {
        final AtomicReference<Runnable> onFirstRemoval = new AtomicReference<>();
        final IndicesFieldDataCache fdCache = newFieldDataCache(onFirstRemoval);
        try (Directory directory = newDirectory(); IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig())) {
            writer.addDocument(new Document());
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                IndexReader.CacheKey readerKey = reader.leaves().get(0).reader().getCoreCacheHelper().getKey();
                Index target = new Index("target", "target-uuid");
                Index other = new Index("other", "other-uuid");
                IndexFieldCache targetField1 = buildIndexFieldCache(fdCache, target, "f1");
                IndexFieldCache targetField2 = buildIndexFieldCache(fdCache, target, "f2");
                IndexFieldCache otherField = buildIndexFieldCache(fdCache, other, "f1");

                Key tailTargetKey = new Key(targetField1, readerKey, null);
                Key fillerKey = new Key(otherField, readerKey, null);
                Key headTargetKey = new Key(targetField2, readerKey, null);
                Accountable value = () -> 10;

                // LRU order after insertion, head to tail: headTargetKey, fillerKey, tailTargetKey
                fdCache.getCache().put(tailTargetKey, value);
                fdCache.getCache().put(fillerKey, value);
                fdCache.getCache().put(headTargetKey, value);

                // While the sweep removes its first entry, a concurrent search hits the
                // not-yet-visited target entry, relinking it at the head of the LRU list
                // behind the sweep's cursor.
                onFirstRemoval.set(() -> fdCache.getCache().get(tailTargetKey));

                fdCache.clear(target);
                fdCache.clear();

                assertEquals(1, fdCache.getCache().count());
                for (Key key : fdCache.getCache().keysSnapshot()) {
                    assertEquals(other, key.indexCache.index);
                }
            }
        } finally {
            fdCache.close();
        }
    }

    /**
     * Same leak through the reader-close path, which was unbounded before this fix: a segment
     * reader closes only once, so an entry whose {@code cacheKeysToClear} mark is consumed by a
     * sweep that then skips it is never reclaimed. Two of three segment readers close; while the
     * sweep removes the first stale entry, a concurrent cache hit promotes the second stale entry
     * behind the cursor. This test fails if the sweep iterates the live {@code Cache#keys()} LRU
     * view and passes with the point-in-time {@code Cache#keysSnapshot()}.
     */
    public void testReaderCloseCleanupIsNotDefeatedByLruPromotionMidSweep() throws Exception {
        final AtomicReference<Runnable> onFirstRemoval = new AtomicReference<>();
        final IndicesFieldDataCache fdCache = newFieldDataCache(onFirstRemoval);
        try (
            Directory directory = newDirectory();
            IndexWriter writer = new IndexWriter(directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))
        ) {
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
                writer.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertEquals(3, reader.leaves().size());
                IndexReader.CacheKey closedKey1 = reader.leaves().get(0).reader().getCoreCacheHelper().getKey();
                IndexReader.CacheKey liveKey = reader.leaves().get(1).reader().getCoreCacheHelper().getKey();
                IndexReader.CacheKey closedKey2 = reader.leaves().get(2).reader().getCoreCacheHelper().getKey();
                IndexFieldCache field = buildIndexFieldCache(fdCache, new Index("index", "uuid"), "f");

                Key tailStaleKey = new Key(field, closedKey1, null);
                Key fillerKey = new Key(field, liveKey, null);
                Key headStaleKey = new Key(field, closedKey2, null);
                Accountable value = () -> 10;

                // LRU order after insertion, head to tail: headStaleKey, fillerKey, tailStaleKey
                fdCache.getCache().put(tailStaleKey, value);
                fdCache.getCache().put(fillerKey, value);
                fdCache.getCache().put(headStaleKey, value);

                // Two segment readers close, marking their entries stale; the third stays live.
                field.onClose(closedKey1);
                field.onClose(closedKey2);

                // While the sweep removes its first stale entry, a concurrent search hits the
                // not-yet-visited stale entry, relinking it at the head of the LRU list behind
                // the sweep's cursor.
                onFirstRemoval.set(() -> fdCache.getCache().get(tailStaleKey));

                fdCache.clear();

                assertEquals(1, fdCache.getCache().count());
                for (Key key : fdCache.getCache().keysSnapshot()) {
                    assertEquals(liveKey, key.readerKey);
                }
            }
        } finally {
            fdCache.close();
        }
    }

    private IndicesFieldDataCache newFieldDataCache(AtomicReference<Runnable> onFirstRemoval) {
        return new IndicesFieldDataCache(Settings.EMPTY, new IndexFieldDataCache.Listener() {
            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
                Runnable hook = onFirstRemoval.getAndSet(null);
                if (hook != null) {
                    hook.run();
                }
            }
        }, null, null);
    }

    private IndexFieldCache buildIndexFieldCache(IndicesFieldDataCache fdCache, Index index, String fieldName) {
        return (IndexFieldCache) fdCache.buildIndexFieldDataCache(new IndexFieldDataCache.Listener() {
        }, index, fieldName);
    }
}
