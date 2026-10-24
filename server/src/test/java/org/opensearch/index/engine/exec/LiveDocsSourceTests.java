/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.CheckedFunction;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Tests for {@link LiveDocsSource#docCountsResolver}.
 */
public class LiveDocsSourceTests extends OpenSearchTestCase {

    /** A reader manager for a format that appends rows and never hides any. */
    private static class PlainReaderManager implements EngineReaderManager<Object> {
        @Override
        public Object getReader(CatalogSnapshot catalogSnapshot) {
            return new Object();
        }

        @Override
        public void beforeRefresh() {}

        @Override
        public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) {}

        @Override
        public void onDeleted(CatalogSnapshot catalogSnapshot) {}

        @Override
        public void onFilesAdded(Collection<String> files) {}

        @Override
        public void onFilesDeleted(Collection<String> files) {}

        @Override
        public void close() {}
    }

    /** A reader manager for the format that holds the liveness information. */
    private static class LiveDocsReaderManager extends PlainReaderManager implements LiveDocsSource {
        private final Map<Long, DocCounts> counts;
        private int calls;

        LiveDocsReaderManager(Map<Long, DocCounts> counts) {
            this.counts = counts;
        }

        @Override
        public Map<Long, DocCounts> docCountsByGeneration(CatalogSnapshot catalogSnapshot) {
            calls++;
            return counts;
        }
    }

    public void testResolverReportsNothingWhenNoFormatHoldsLiveness() throws IOException {
        CheckedFunction<CatalogSnapshot, Map<Long, DocCounts>, IOException> resolver = LiveDocsSource.docCountsResolver(
            List.of(new PlainReaderManager(), new PlainReaderManager())
        );

        assertTrue(resolver.apply(mock(CatalogSnapshot.class)).isEmpty());
    }

    public void testResolverReportsNothingForAnEmptyShard() throws IOException {
        CheckedFunction<CatalogSnapshot, Map<Long, DocCounts>, IOException> resolver = LiveDocsSource.docCountsResolver(List.of());

        assertTrue(resolver.apply(mock(CatalogSnapshot.class)).isEmpty());
    }

    public void testResolverPassesThroughTheCountsFromTheLivenessHoldingFormat() throws IOException {
        LiveDocsReaderManager liveDocs = new LiveDocsReaderManager(Map.of(1L, new DocCounts(4, 6), 2L, new DocCounts(3, 0)));
        CheckedFunction<CatalogSnapshot, Map<Long, DocCounts>, IOException> resolver = LiveDocsSource.docCountsResolver(
            List.of(new PlainReaderManager(), liveDocs)
        );

        Map<Long, DocCounts> counts = resolver.apply(mock(CatalogSnapshot.class));

        assertEquals(2, counts.size());
        assertEquals(new DocCounts(4, 6), counts.get(1L));
        assertEquals(new DocCounts(3, 0), counts.get(2L));
    }

    public void testResolverIsAskedOncePerSnapshotNotOncePerManager() throws IOException {
        LiveDocsReaderManager liveDocs = new LiveDocsReaderManager(Map.of(1L, new DocCounts(2, 0)));
        CheckedFunction<CatalogSnapshot, Map<Long, DocCounts>, IOException> resolver = LiveDocsSource.docCountsResolver(
            List.of(new PlainReaderManager(), liveDocs, new PlainReaderManager())
        );

        resolver.apply(mock(CatalogSnapshot.class));
        resolver.apply(mock(CatalogSnapshot.class));

        assertEquals(2, liveDocs.calls);
    }

    public void testResolverRejectsMoreThanOneLivenessHoldingFormat() {
        assertTrue("this test relies on assertions being enabled", LiveDocsSourceTests.class.desiredAssertionStatus());

        AssertionError error = expectThrows(
            AssertionError.class,
            () -> LiveDocsSource.docCountsResolver(List.of(new LiveDocsReaderManager(Map.of()), new LiveDocsReaderManager(Map.of())))
        );
        assertTrue(error.getMessage(), error.getMessage().contains("at most one data format"));
    }
}
