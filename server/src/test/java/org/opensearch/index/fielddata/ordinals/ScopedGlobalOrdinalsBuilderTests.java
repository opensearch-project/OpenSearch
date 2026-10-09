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
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.index.fielddata.IndexOrdinalsFieldData;
import org.opensearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.opensearch.indices.breaker.HierarchyCircuitBreakerService;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the group-scoped global ordinals build ({@link GlobalOrdinalsBuilder#buildScoped}), which builds an
 * {@link org.apache.lucene.index.OrdinalMap} over only the join-field terms starting with a given prefix. This is an
 * opt-in optimisation for indices that store documents from many groups in one index, whose ids are prefixed with a
 * group id, and that issue group-scoped {@code has_child}/{@code has_parent} queries.
 */
public class ScopedGlobalOrdinalsBuilderTests extends OpenSearchTestCase {

    private static final Runnable NO_CANCELLATION = () -> {};

    /**
     * The scoped build reserves FIELDDATA breaker bytes and, because the scoped map is never cached, exposes a
     * {@link Releasable} that must return those bytes when the search completes. Verify used &gt; 0 after build and
     * back to baseline after release, repeated many times to prove there is no accumulation (the leak we fixed).
     */
    public void testScopedBuildReservesAndReleasesFieldDataBreaker() throws IOException {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir);
            w.w.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
            // Two groups ("a:" and "b:") spread across multiple segments.
            for (int seg = 0; seg < 3; seg++) {
                for (int i = 0; i < 5; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("field", new BytesRef("a:seg" + seg + "_" + i)));
                    w.addDocument(doc);
                    Document doc2 = new Document();
                    doc2.add(new SortedSetDocValuesField("field", new BytesRef("b:seg" + seg + "_" + i)));
                    w.addDocument(doc2);
                }
                w.flush();
            }

            try (IndexReader reader = w.getReader()) {
                w.close();
                assertTrue("need multiple segments", reader.leaves().size() > 1);
                IndexOrdinalsFieldData fieldData = mockFieldData("field", reader);
                HierarchyCircuitBreakerService breakerService = newBreakerService("100mb");
                CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.FIELDDATA);

                long baseline = breaker.getUsed();
                for (int iter = 0; iter < 25; iter++) {
                    IndexOrdinalsFieldData scoped = GlobalOrdinalsBuilder.buildScoped(
                        reader,
                        fieldData,
                        breakerService,
                        logger,
                        AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION,
                        new BytesRef("a:"),
                        NO_CANCELLATION
                    );
                    assertTrue("breaker should be reserved during scoped build", breaker.getUsed() > baseline);
                    Releasable releasable = ((GlobalOrdinalsIndexFieldData) scoped).getBreakerReleasable();
                    assertNotNull("scoped fielddata must expose a breaker releasable", releasable);
                    releasable.close();
                    assertEquals("breaker must return to baseline after release", baseline, breaker.getUsed());
                }
                // No accumulation across many builds.
                assertEquals(baseline, breaker.getUsed());
            }
        }
    }

    /**
     * Releasing the reserved breaker bytes must be idempotent: closing the releasable more than once must not
     * double-subtract (which would drive the breaker negative).
     */
    public void testScopedBreakerReleaseIsIdempotent() throws IOException {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir);
            w.w.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
            for (int seg = 0; seg < 2; seg++) {
                for (int i = 0; i < 4; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("field", new BytesRef("a:seg" + seg + "_" + i)));
                    w.addDocument(doc);
                    Document doc2 = new Document();
                    doc2.add(new SortedSetDocValuesField("field", new BytesRef("b:seg" + seg + "_" + i)));
                    w.addDocument(doc2);
                }
                w.flush();
            }

            try (IndexReader reader = w.getReader()) {
                w.close();
                assertTrue(reader.leaves().size() > 1);
                IndexOrdinalsFieldData fieldData = mockFieldData("field", reader);
                HierarchyCircuitBreakerService breakerService = newBreakerService("100mb");
                CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.FIELDDATA);
                long baseline = breaker.getUsed();

                IndexOrdinalsFieldData scoped = GlobalOrdinalsBuilder.buildScoped(
                    reader,
                    fieldData,
                    breakerService,
                    logger,
                    AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION,
                    new BytesRef("a:"),
                    NO_CANCELLATION
                );
                Releasable releasable = ((GlobalOrdinalsIndexFieldData) scoped).getBreakerReleasable();
                releasable.close();
                releasable.close(); // second close must be a no-op
                assertEquals("double release must not drive breaker below baseline", baseline, breaker.getUsed());
            }
        }
    }

    /**
     * A shard/segment set that contains no terms for the requested prefix must not throw; the scoped build returns an
     * empty ordinal view (the caller short-circuits such a shard to "match none").
     */
    public void testScopedBuildWithAbsentPrefixDoesNotThrow() throws IOException {
        try (Directory dir = newDirectory()) {
            RandomIndexWriter w = new RandomIndexWriter(random(), dir);
            w.w.getConfig().setMergePolicy(NoMergePolicy.INSTANCE);
            for (int seg = 0; seg < 2; seg++) {
                for (int i = 0; i < 4; i++) {
                    Document doc = new Document();
                    doc.add(new SortedSetDocValuesField("field", new BytesRef("a:seg" + seg + "_" + i)));
                    w.addDocument(doc);
                }
                w.flush();
            }

            try (IndexReader reader = w.getReader()) {
                w.close();
                assertTrue(reader.leaves().size() > 1);
                IndexOrdinalsFieldData fieldData = mockFieldData("field", reader);
                HierarchyCircuitBreakerService breakerService = newBreakerService("100mb");
                CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.FIELDDATA);
                long baseline = breaker.getUsed();

                // Prefix "z:" matches no terms on this shard.
                IndexOrdinalsFieldData scoped = GlobalOrdinalsBuilder.buildScoped(
                    reader,
                    fieldData,
                    breakerService,
                    logger,
                    AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION,
                    new BytesRef("z:"),
                    NO_CANCELLATION
                );
                assertNotNull(scoped);
                Releasable releasable = ((GlobalOrdinalsIndexFieldData) scoped).getBreakerReleasable();
                if (releasable != null) {
                    releasable.close();
                }
                assertEquals(baseline, breaker.getUsed());
            }
        }
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
                public java.util.Collection<org.apache.lucene.util.Accountable> getChildResources() {
                    return Collections.emptyList();
                }

                @Override
                public void close() {}
            };
        });
        return fieldData;
    }
}
