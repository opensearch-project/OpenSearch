/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata.ordinals;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.fielddata.IndexOrdinalsFieldData;
import org.opensearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link ScopedGlobalOrdinalsCache}, the dedicated node-level cache for group-scoped global ordinals used by
 * the {@code scope_prefix} optimisation. The cache must: reuse one build across repeated lookups for the same
 * (reader, prefix), keep distinct prefixes separate, evict per reader generation, and release the reserved FIELDDATA
 * circuit-breaker bytes exactly once when an entry is removed (eviction, reader close, or clear).
 */
public class ScopedGlobalOrdinalsCacheTests extends OpenSearchTestCase {

    private static final Runnable NO_CANCELLATION = () -> {};
    private static final Index INDEX = new Index("test-index", "uuid");
    private static final ShardId SHARD = new ShardId(INDEX, 0);
    private static final String FIELD = "field";

    public void testMissThenHitBuildsOnlyOnce() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexReader reader = writeTwoGroups(dir);
            try (reader) {
                HierarchyCircuitBreakerService breakerService = newBreakerService("100mb");
                AtomicInteger builds = new AtomicInteger();
                IndexOrdinalsFieldData fieldData = mockFieldData(FIELD, reader);

                try (ScopedGlobalOrdinalsCache cache = new ScopedGlobalOrdinalsCache(new ByteSizeValue(50, ByteSizeUnit.MB), null)) {
                    Object readerKey = reader.getReaderCacheHelper().getKey();
                    IndexOrdinalsFieldData first = cache.computeIfAbsent(
                        INDEX,
                        FIELD,
                        SHARD,
                        readerKey,
                        new BytesRef("a:"),
                        () -> buildScoped(reader, fieldData, breakerService, builds, "a:")
                    );
                    IndexOrdinalsFieldData second = cache.computeIfAbsent(
                        INDEX,
                        FIELD,
                        SHARD,
                        readerKey,
                        new BytesRef("a:"),
                        () -> buildScoped(reader, fieldData, breakerService, builds, "a:")
                    );

                    assertEquals("second lookup for same key must be a cache hit", 1, builds.get());
                    assertSame("cache must return the same cached instance", first, second);
                    assertEquals(1, cache.count());
                }
            }
        }
    }

    public void testDifferentPrefixesAreCachedSeparately() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexReader reader = writeTwoGroups(dir);
            try (reader) {
                HierarchyCircuitBreakerService breakerService = newBreakerService("100mb");
                AtomicInteger builds = new AtomicInteger();
                IndexOrdinalsFieldData fieldData = mockFieldData(FIELD, reader);

                try (ScopedGlobalOrdinalsCache cache = new ScopedGlobalOrdinalsCache(new ByteSizeValue(50, ByteSizeUnit.MB), null)) {
                    Object readerKey = reader.getReaderCacheHelper().getKey();
                    cache.computeIfAbsent(
                        INDEX,
                        FIELD,
                        SHARD,
                        readerKey,
                        new BytesRef("a:"),
                        () -> buildScoped(reader, fieldData, breakerService, builds, "a:")
                    );
                    cache.computeIfAbsent(
                        INDEX,
                        FIELD,
                        SHARD,
                        readerKey,
                        new BytesRef("b:"),
                        () -> buildScoped(reader, fieldData, breakerService, builds, "b:")
                    );

                    assertEquals("distinct prefixes must each build once", 2, builds.get());
                    assertEquals(2, cache.count());
                }
            }
        }
    }

    public void testInvalidateReaderEvictsAndReleasesBreaker() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexReader reader = writeTwoGroups(dir);
            try (reader) {
                HierarchyCircuitBreakerService breakerService = newBreakerService("100mb");
                CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.FIELDDATA);
                AtomicInteger builds = new AtomicInteger();
                IndexOrdinalsFieldData fieldData = mockFieldData(FIELD, reader);
                long baseline = breaker.getUsed();

                try (ScopedGlobalOrdinalsCache cache = new ScopedGlobalOrdinalsCache(new ByteSizeValue(50, ByteSizeUnit.MB), null)) {
                    Object readerKey = reader.getReaderCacheHelper().getKey();
                    cache.computeIfAbsent(
                        INDEX,
                        FIELD,
                        SHARD,
                        readerKey,
                        new BytesRef("a:"),
                        () -> buildScoped(reader, fieldData, breakerService, builds, "a:")
                    );
                    assertTrue("breaker reserved while cached", breaker.getUsed() > baseline);
                    assertEquals(1, cache.count());

                    cache.invalidateReader(readerKey);
                    assertEquals("reader eviction must remove the entry", 0, cache.count());
                    assertEquals("reader eviction must release the reserved breaker bytes", baseline, breaker.getUsed());
                }
            }
        }
    }

    public void testCloseReleasesAllBreakerBytes() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexReader reader = writeTwoGroups(dir);
            try (reader) {
                HierarchyCircuitBreakerService breakerService = newBreakerService("100mb");
                CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.FIELDDATA);
                AtomicInteger builds = new AtomicInteger();
                IndexOrdinalsFieldData fieldData = mockFieldData(FIELD, reader);
                long baseline = breaker.getUsed();

                ScopedGlobalOrdinalsCache cache = new ScopedGlobalOrdinalsCache(new ByteSizeValue(50, ByteSizeUnit.MB), null);
                Object readerKey = reader.getReaderCacheHelper().getKey();
                cache.computeIfAbsent(
                    INDEX,
                    FIELD,
                    SHARD,
                    readerKey,
                    new BytesRef("a:"),
                    () -> buildScoped(reader, fieldData, breakerService, builds, "a:")
                );
                cache.computeIfAbsent(
                    INDEX,
                    FIELD,
                    SHARD,
                    readerKey,
                    new BytesRef("b:"),
                    () -> buildScoped(reader, fieldData, breakerService, builds, "b:")
                );
                assertTrue(breaker.getUsed() > baseline);

                cache.close();
                assertEquals("close must release all reserved breaker bytes", baseline, breaker.getUsed());
                assertEquals(0, cache.count());
            }
        }
    }

    public void testSizeEvictionReleasesBreaker() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexReader reader = writeTwoGroups(dir);
            try (reader) {
                HierarchyCircuitBreakerService breakerService = newBreakerService("100mb");
                CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.FIELDDATA);
                AtomicInteger builds = new AtomicInteger();
                IndexOrdinalsFieldData fieldData = mockFieldData(FIELD, reader);
                long baseline = breaker.getUsed();

                // Build one entry first to learn its weight, so we can size the cache to hold exactly one.
                long oneEntryWeight;
                try (ScopedGlobalOrdinalsCache probe = new ScopedGlobalOrdinalsCache(new ByteSizeValue(50, ByteSizeUnit.MB), null)) {
                    probe.computeIfAbsent(
                        INDEX,
                        FIELD,
                        SHARD,
                        reader.getReaderCacheHelper().getKey(),
                        new BytesRef("a:"),
                        () -> buildScoped(reader, fieldData, breakerService, builds, "a:")
                    );
                    oneEntryWeight = probe.ramBytesUsed();
                }
                assertTrue("scoped entry should have non-zero weight", oneEntryWeight > 0);

                // Cache bounded to a single entry: inserting a second must evict the first (LRU) and release its bytes.
                try (ScopedGlobalOrdinalsCache cache = new ScopedGlobalOrdinalsCache(new ByteSizeValue(oneEntryWeight), null)) {
                    Object readerKey = reader.getReaderCacheHelper().getKey();
                    cache.computeIfAbsent(
                        INDEX,
                        FIELD,
                        SHARD,
                        readerKey,
                        new BytesRef("a:"),
                        () -> buildScoped(reader, fieldData, breakerService, builds, "a:")
                    );
                    cache.computeIfAbsent(
                        INDEX,
                        FIELD,
                        SHARD,
                        readerKey,
                        new BytesRef("b:"),
                        () -> buildScoped(reader, fieldData, breakerService, builds, "b:")
                    );

                    assertEquals("cache bounded to one entry", 1, cache.count());
                    // The evicted entry's breaker bytes must be released: the breaker should now hold exactly the
                    // baseline plus the single surviving entry's weight (the probe cache was already closed).
                    assertEquals(
                        "size eviction must release the evicted entry's breaker bytes",
                        baseline + cache.ramBytesUsed(),
                        breaker.getUsed()
                    );
                    // Sanity: the reserved total is far below what two entries would need.
                    assertTrue(breaker.getUsed() < baseline + 2 * oneEntryWeight);
                }
                assertEquals("closing cache releases the remaining entry too", baseline, breaker.getUsed());
            }
        }
    }

    private IndexOrdinalsFieldData buildScoped(
        IndexReader reader,
        IndexOrdinalsFieldData fieldData,
        HierarchyCircuitBreakerService breakerService,
        AtomicInteger buildCounter,
        String prefix
    ) {
        buildCounter.incrementAndGet();
        try {
            return GlobalOrdinalsBuilder.buildScoped(
                reader,
                fieldData,
                breakerService,
                logger,
                AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION,
                new BytesRef(prefix),
                NO_CANCELLATION
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private IndexReader writeTwoGroups(Directory dir) throws IOException {
        RandomIndexWriter w = new RandomIndexWriter(random(), dir);
        w.w.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        for (int seg = 0; seg < 3; seg++) {
            for (int i = 0; i < 5; i++) {
                Document doc = new Document();
                doc.add(new SortedSetDocValuesField(FIELD, new BytesRef("a:seg" + seg + "_" + i)));
                w.addDocument(doc);
                Document doc2 = new Document();
                doc2.add(new SortedSetDocValuesField(FIELD, new BytesRef("b:seg" + seg + "_" + i)));
                w.addDocument(doc2);
            }
            w.flush();
        }
        IndexReader reader = w.getReader();
        w.close();
        return reader;
    }

    private static HierarchyCircuitBreakerService newBreakerService(String fielddataLimit) {
        return new HierarchyCircuitBreakerService(
            Settings.builder()
                .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
                .put(HierarchyCircuitBreakerService.FIELDDATA_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), fielddataLimit)
                .build(),
            Collections.emptyList(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
    }

    private static IndexOrdinalsFieldData mockFieldData(String fieldName, IndexReader reader) {
        IndexOrdinalsFieldData fieldData = mock(IndexOrdinalsFieldData.class);
        when(fieldData.getFieldName()).thenReturn(fieldName);
        when(fieldData.load(any(LeafReaderContext.class))).thenAnswer(invocation -> {
            LeafReaderContext ctx = invocation.getArgument(0);
            return new AbstractLeafOrdinalsFieldData(AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION) {
                @Override
                public SortedSetDocValues getOrdinalsValues() {
                    try {
                        SortedSetDocValues dv = ctx.reader().getSortedSetDocValues(fieldName);
                        return dv != null ? dv : DocValues.emptySortedSet();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

                @Override
                public long ramBytesUsed() {
                    return 0;
                }

                @Override
                public void close() {}
            };
        });
        return fieldData;
    }
}
