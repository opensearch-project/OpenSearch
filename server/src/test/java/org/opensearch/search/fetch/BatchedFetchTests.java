/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.index.BatchPlan;
import org.opensearch.common.lucene.index.BatchRequest;
import org.opensearch.common.lucene.index.BatchedDocumentValues;
import org.opensearch.common.lucene.index.BatchedDocumentValuesProvider;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.profile.fetch.FetchProfileBreakdown;
import org.opensearch.search.profile.fetch.FetchTimingType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BatchedFetchTests extends OpenSearchTestCase {

    /** Two segments: three docs (global 0-2), then four docs (global 3-6). */
    private Directory directory;
    private DirectoryReader plain;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = newDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
            for (int segment = 0; segment < 2; segment++) {
                for (int i = 0; i < 3 + segment; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("id", segment + "-" + i, Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
        }
        plain = DirectoryReader.open(directory);
        assertEquals(2, plain.leaves().size());
    }

    @Override
    public void tearDown() throws Exception {
        plain.close();
        directory.close();
        super.tearDown();
    }

    public void testLeafDocsAreTheSegmentsHitsWithoutRepeats() {
        FetchPhase.DocIdToIndex[] docs = hits(0, 2, 2, 3, 5, 6);
        assertArrayEquals(new int[] { 0, 2 }, BatchedFetch.leafDocs(plain.leaves().get(0), docs, 0));
        assertArrayEquals(new int[] { 0, 2, 3 }, BatchedFetch.leafDocs(plain.leaves().get(1), docs, 3));
    }

    public void testFindsTheProviderBeneathFilterReaders() throws IOException {
        List<String> events = new ArrayList<>();
        LeafReader segment = plain.leaves().get(0).reader();
        RecordingProvider provider = new RecordingProvider(segment, "s0", events, Mode.SERVE);
        assertSame(provider, BatchedFetch.findProvider(new PassThrough(new PassThrough(provider))));
        assertNull("a reader with no provider beneath it reads one document at a time", BatchedFetch.findProvider(segment));
    }

    public void testOpensOneSegmentBatchAtATime() throws IOException {
        List<String> events = new ArrayList<>();
        try (DirectoryReader reader = wrap(plain, events, Mode.SERVE)) {
            FetchPhase.DocIdToIndex[] docs = hits(1, 2, 4, 6);
            BatchedFetch fetch = new BatchedFetch(context(false));
            fetch.start(true, null);
            fetch.onSegment(reader.leaves().get(0), docs, 0);
            assertEquals(List.of("plan s0 [1, 2]", "open s0"), events);
            fetch.onSegment(reader.leaves().get(1), docs, 2);
            assertEquals(List.of("plan s0 [1, 2]", "open s0", "close s0", "release s0", "plan s1 [1, 3]", "open s1"), events);
            fetch.close();
            fetch.close();
            assertEquals(
                List.of("plan s0 [1, 2]", "open s0", "close s0", "release s0", "plan s1 [1, 3]", "open s1", "close s1", "release s1"),
                events
            );
        }
    }

    public void testTimesOnlySegmentsThatBatch() throws IOException {
        FetchProfileBreakdown breakdown = new FetchProfileBreakdown();
        BatchedFetch fetch = new BatchedFetch(context(false));
        fetch.start(true, breakdown);
        fetch.onSegment(plain.leaves().get(0), hits(0), 0);
        assertEquals(0, breakdown.getTimer(FetchTimingType.LOAD_STORED_FIELDS).getCount());

        try (DirectoryReader reader = wrap(plain, new ArrayList<>(), Mode.SERVE)) {
            fetch.onSegment(reader.leaves().get(0), hits(0), 0);
            fetch.close();
        }
        assertEquals(1, breakdown.getTimer(FetchTimingType.LOAD_STORED_FIELDS).getCount());
    }

    public void testDeclinedPlanOpensNothing() throws IOException {
        List<String> events = new ArrayList<>();
        try (DirectoryReader reader = wrap(plain, events, Mode.DECLINE)) {
            BatchedFetch fetch = new BatchedFetch(context(false));
            fetch.start(true, null);
            fetch.onSegment(reader.leaves().get(0), hits(0), 0);
            fetch.close();
            assertEquals(List.of("plan s0 [0]"), events);
        }
    }

    public void testProviderFailuresFailTheFetch() throws IOException {
        List<String> events = new ArrayList<>();
        try (DirectoryReader reader = wrap(plain, events, Mode.FAIL_OPEN)) {
            BatchedFetch fetch = new BatchedFetch(context(false));
            fetch.start(true, null);
            expectThrows(UncheckedIOException.class, () -> fetch.onSegment(reader.leaves().get(0), hits(0), 0));
            fetch.close();
            assertEquals("a plan whose open failed is released", List.of("plan s0 [0]", "open s0", "release s0"), events);
        }
        events.clear();
        try (DirectoryReader reader = wrap(plain, events, Mode.FAIL_PLAN)) {
            BatchedFetch fetch = new BatchedFetch(context(false));
            fetch.start(true, null);
            expectThrows(IOException.class, () -> fetch.onSegment(reader.leaves().get(0), hits(0), 0));
            fetch.close();
            assertEquals(List.of("plan s0 [0]"), events);
        }
    }

    public void testBatchesOnlyFetchesThatDeriveSource() throws IOException {
        List<String> events = new ArrayList<>();
        try (DirectoryReader reader = wrap(plain, events, Mode.SERVE)) {
            BatchedFetch noSource = new BatchedFetch(context(false));
            noSource.start(false, null);
            noSource.onSegment(reader.leaves().get(0), hits(0), 0);

            BatchedFetch nested = new BatchedFetch(context(true));
            nested.start(true, null);
            nested.onSegment(reader.leaves().get(0), hits(0), 0);
            assertEquals(List.of(), events);
        }
    }

    private static FetchPhase.DocIdToIndex[] hits(int... globalDocs) {
        FetchPhase.DocIdToIndex[] docs = new FetchPhase.DocIdToIndex[globalDocs.length];
        for (int i = 0; i < globalDocs.length; i++) {
            docs[i] = new FetchPhase.DocIdToIndex(globalDocs[i], i);
        }
        return docs;
    }

    private static SearchContext context(boolean hasNested) {
        SearchContext context = mock(SearchContext.class);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.hasNested()).thenReturn(hasNested);
        when(context.mapperService()).thenReturn(mapperService);
        when(context.isCancelled()).thenReturn(false);
        return context;
    }

    /** Wraps every segment in a recording provider beneath a pass-through wrapper, as a security wrapper would sit. */
    private static DirectoryReader wrap(DirectoryReader in, List<String> events, Mode mode) throws IOException {
        return new FilterDirectoryReader(in, new FilterDirectoryReader.SubReaderWrapper() {
            private int segment;

            @Override
            public LeafReader wrap(LeafReader reader) {
                return new PassThrough(new RecordingProvider(reader, "s" + segment++, events, mode));
            }
        }) {
            @Override
            protected DirectoryReader doWrapDirectoryReader(DirectoryReader reader) {
                throw new UnsupportedOperationException();
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
            }

            @Override
            protected void doClose() {
                // The plain reader is closed by the test.
            }
        };
    }

    private enum Mode {
        SERVE,
        DECLINE,
        FAIL_PLAN,
        FAIL_OPEN
    }

    private static class PassThrough extends FilterLeafReader {
        PassThrough(LeafReader in) {
            super(in);
        }

        @Override
        public CacheHelper getCoreCacheHelper() {
            return in.getCoreCacheHelper();
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
            return in.getReaderCacheHelper();
        }
    }

    private static final class RecordingProvider extends PassThrough implements BatchedDocumentValuesProvider {
        private final String name;
        private final List<String> events;
        private final Mode mode;

        RecordingProvider(LeafReader in, String name, List<String> events, Mode mode) {
            super(in);
            this.name = name;
            this.events = events;
            this.mode = mode;
        }

        @Override
        public BatchPlan plan(BatchRequest request) throws IOException {
            events.add("plan " + name + " " + Arrays.toString(request.docs()));
            return switch (mode) {
                case DECLINE -> null;
                case FAIL_PLAN -> throw new IOException("plan failed");
                default -> () -> events.add("release " + name);
            };
        }

        @Override
        public BatchedDocumentValues open(BatchPlan plan) {
            events.add("open " + name);
            if (mode == Mode.FAIL_OPEN) {
                throw new UncheckedIOException(new IOException("read failed"));
            }
            return () -> events.add("close " + name);
        }
    }
}
