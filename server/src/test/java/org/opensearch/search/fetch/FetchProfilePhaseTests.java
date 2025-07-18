/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.common.text.Text;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.search.SearchHits;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.Profilers;
import org.opensearch.search.profile.fetch.FetchProfiler;
import org.opensearch.search.profile.fetch.FetchTimingType;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FetchProfilePhaseTests extends IndexShardTestCase {
    private IndexShard indexShard;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        indexShard = newShard(true);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        closeShards(indexShard);
    }

    private static class TestDocumentBuilder {
        private final List<Document> documents = new ArrayList<>();

        TestDocumentBuilder addDocuments(int count, boolean includeSource) {
            for (int i = 0; i < count; i++) {
                Document doc = new Document();
                doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
                doc.add(new TextField("content", "test content " + i, Field.Store.YES));

                if (includeSource) {
                    String sourceJson = "{\"id\":" + i + ",\"content\":\"test content " + i + "\"}";
                    doc.add(new org.apache.lucene.document.StoredField("_source", sourceJson.getBytes(StandardCharsets.UTF_8)));
                }

                documents.add(doc);
            }
            return this;
        }

        List<Document> build() {
            return documents;
        }
    }

    private static class TimingAssertions {
        private final Map<String, Long> breakdown;

        TimingAssertions(Map<String, Long> breakdown) {
            this.breakdown = breakdown;
        }

        TimingAssertions assertTimingPresent(FetchTimingType timingType) {
            String timingKey = timingType.toString();
            String countKey = timingKey + "_count";
            String startTimeKey = timingKey + "_start_time";

            assertTrue("Timing key should be present: " + timingKey, breakdown.containsKey(timingKey));
            assertTrue("Count key should be present: " + countKey, breakdown.containsKey(countKey));

            assertThat("Timing should be geq 1: " + timingKey, breakdown.get(timingKey), greaterThanOrEqualTo(1L));
            assertThat("Count should be non-negative: " + countKey, breakdown.get(countKey), greaterThanOrEqualTo(0L));
            return this;
        }

        TimingAssertions assertBreakdownNotEmpty() {
            assertThat("Profile breakdown should not be empty", breakdown.size(), greaterThan(0));
            return this;
        }
    }

    private static class SearchContextBuilder {
        private final IndexReader reader;
        private final int[] docIds;
        private final IndexShard indexShard;

        private boolean enableSourceLoading = false;
        private boolean enableStoredFields = false;
        private List<String> storedFieldNames = Collections.emptyList();
        private boolean enableScriptFields = false;

        SearchContextBuilder(IndexReader reader, int[] docIds, IndexShard indexShard) {
            this.reader = reader;
            this.docIds = docIds;
            this.indexShard = indexShard;
        }

        SearchContextBuilder withSourceLoading() {
            this.enableSourceLoading = true;
            return this;
        }

        SearchContextBuilder withStoredFields(String... fieldNames) {
            this.enableStoredFields = true;
            this.storedFieldNames = Arrays.asList(fieldNames);
            return this;
        }

        SearchContextBuilder withScriptFields() {
            this.enableScriptFields = true;
            return this;
        }

        SearchContext build() throws IOException {
            SearchContext context = mock(SearchContext.class);

            // Basic document loading configuration
            when(context.docIdsToLoadSize()).thenReturn(docIds.length);
            when(context.docIdsToLoad()).thenReturn(docIds);
            when(context.docIdsToLoadFrom()).thenReturn(0);
            when(context.isCancelled()).thenReturn(false);

            // Script fields configuration
            when(context.hasScriptFields()).thenReturn(enableScriptFields);

            // Source loading configuration
            when(context.hasFetchSourceContext()).thenReturn(enableSourceLoading);
            when(context.sourceRequested()).thenReturn(enableSourceLoading);
            if (enableSourceLoading) {
                when(context.fetchSourceContext()).thenReturn(new FetchSourceContext(true));
            }

            // Stored fields configuration
            if (enableStoredFields && !storedFieldNames.isEmpty()) {
                StoredFieldsContext storedFieldsContext = StoredFieldsContext.fromList(storedFieldNames);
                when(context.storedFieldsContext()).thenReturn(storedFieldsContext);
            } else {
                when(context.storedFieldsContext()).thenReturn(null);
            }

            // Query result setup
            QuerySearchResult queryResult = new QuerySearchResult();
            ScoreDoc[] scoreDocs = new ScoreDoc[docIds.length];
            for (int i = 0; i < docIds.length; i++) {
                scoreDocs[i] = new ScoreDoc(docIds[i], 1.0f);
            }
            TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(
                new org.apache.lucene.search.TopDocs(new TotalHits(docIds.length, TotalHits.Relation.EQUAL_TO), scoreDocs),
                1.0f
            );
            queryResult.topDocs(topDocsAndMaxScore, null);
            when(context.queryResult()).thenReturn(queryResult);

            // Search lookup setup
            QueryShardContext queryShardContext = mock(QueryShardContext.class);
            SearchLookup searchLookup = mock(SearchLookup.class);
            SourceLookup sourceLookup = new SourceLookup();
            when(searchLookup.source()).thenReturn(sourceLookup);
            when(queryShardContext.newFetchLookup()).thenReturn(searchLookup);
            when(context.getQueryShardContext()).thenReturn(queryShardContext);

            // Mapper service setup
            MapperService mapperService = mock(MapperService.class);
            when(mapperService.hasNested()).thenReturn(false);
            DocumentMapper documentMapper = mock(DocumentMapper.class);
            when(documentMapper.typeText()).thenReturn(new Text("_doc"));
            when(mapperService.documentMapper()).thenReturn(documentMapper);
            when(context.mapperService()).thenReturn(mapperService);

            // Index searcher setup
            ContextIndexSearcher searcher = new ContextIndexSearcher(
                reader,
                IndexSearcher.getDefaultSimilarity(),
                IndexSearcher.getDefaultQueryCache(),
                IndexSearcher.getDefaultQueryCachingPolicy(),
                true,
                null,
                context
            );
            when(context.searcher()).thenReturn(searcher);
            when(context.indexShard()).thenReturn(indexShard);

            // Fetch result setup
            FetchSearchResult fetchResult = new FetchSearchResult();
            when(context.fetchResult()).thenReturn(fetchResult);

            // Profiling setup
            Profilers profilers = new Profilers(searcher, false);
            when(context.getProfilers()).thenReturn(profilers);

            return context;
        }
    }

    // Helper method to execute fetch phase and get profile results
    private ProfileResult executeFetchPhaseAndGetProfile(SearchContext context, List<FetchSubPhase> subPhases) throws IOException {
        FetchPhase fetchPhase = new FetchPhase(subPhases);
        fetchPhase.execute(context);

        FetchProfiler fetchProfiler = context.getProfilers().getFetchProfiler();
        List<ProfileResult> profileResults = fetchProfiler.getTree();
        assertThat(profileResults, hasSize(greaterThanOrEqualTo(1)));

        return profileResults.get(0);
    }

    // Helper method to index documents and get their IDs
    private int[] indexDocumentsAndGetIds(Directory dir, List<Document> docs, int maxResults) throws IOException {
        try (RandomIndexWriter w = new RandomIndexWriter(random(), dir, new IndexWriterConfig(new StandardAnalyzer()))) {
            for (Document doc : docs) {
                w.addDocument(doc);
            }
        }

        try (IndexReader reader = DirectoryReader.open(dir)) {
            IndexSearcher searcher = new IndexSearcher(reader);
            TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), maxResults);

            int[] docIds = new int[Math.min(maxResults, topDocs.scoreDocs.length)];
            for (int i = 0; i < docIds.length; i++) {
                docIds[i] = topDocs.scoreDocs[i].doc;
            }
            return docIds;
        }
    }

    // Mock fetch sub-phase for testing
    private FetchSubPhase createMockFetchSubPhase() {
        return new FetchSubPhase() {
            @Override
            public FetchSubPhaseProcessor getProcessor(FetchContext fetchContext) {
                return new FetchSubPhaseProcessor() {
                    @Override
                    public void setNextReader(org.apache.lucene.index.LeafReaderContext readerContext) {
                        // Mock implementation
                    }

                    @Override
                    public void process(HitContext hitContext) {
                        // Mock implementation - add a simple field
                        hitContext.hit()
                            .setDocumentField(
                                "mock_field",
                                new org.opensearch.common.document.DocumentField("mock_field", Collections.singletonList("mock_value"))
                            );
                    }
                };
            }
        };
    }

    public void testRootTiming() throws Exception {
        try (Directory dir = newDirectory()) {
            List<Document> docs = new TestDocumentBuilder().addDocuments(3, true).build();
            int[] docIds = indexDocumentsAndGetIds(dir, docs, 2);

            try (IndexReader reader = DirectoryReader.open(dir)) {
                SearchContext context = new SearchContextBuilder(reader, docIds, indexShard).withSourceLoading()
                    .withStoredFields("_source")
                    .build();

                ProfileResult profile = executeFetchPhaseAndGetProfile(context, Collections.emptyList());

                new TimingAssertions(profile.getTimeBreakdown()).assertBreakdownNotEmpty()
                    .assertTimingPresent(FetchTimingType.CREATE_STORED_FIELDS_VISITOR)
                    .assertTimingPresent(FetchTimingType.LOAD_SOURCE)
                    .assertTimingPresent(FetchTimingType.LOAD_STORED_FIELDS)
                    .assertTimingPresent(FetchTimingType.BUILD_SEARCH_HITS)
                    .assertTimingPresent(FetchTimingType.BUILD_SUB_PHASE_PROCESSORS)
                    .assertTimingPresent(FetchTimingType.NEXT_READER);
            }
        }
    }


//    public void testComplexFetchScenarioWithAllTimings() throws Exception {
//        try (Directory dir = newDirectory()) {
//            List<Document> docs = new TestDocumentBuilder().addDocuments(6, true).build();
//            int[] docIds = indexDocumentsAndGetIds(dir, docs, 4);
//
//            try (IndexReader reader = DirectoryReader.open(dir)) {
//                SearchContext context = new SearchContextBuilder(reader, docIds, indexShard).withSourceLoading()
//                    .withStoredFields("_source", "id", "content")
//                    .build();
//
//                List<FetchSubPhase> subPhases = Collections.singletonList(createMockFetchSubPhase());
//                ProfileResult profile = executeFetchPhaseAndGetProfile(context, subPhases);
//
//                new TimingAssertions(profile.getTimeBreakdown()).assertBreakdownNotEmpty()
//                    .assertTimingExecuted(FetchTimingType.CREATE_STORED_FIELDS_VISITOR)
//                    .assertTimingExecuted(FetchTimingType.BUILD_SUB_PHASE_PROCESSORS)
//                    .assertTimingExecuted(FetchTimingType.LOAD_STORED_FIELDS)
//                    .assertTimingExecuted(FetchTimingType.LOAD_SOURCE)
//                    .assertTimingExecuted(FetchTimingType.BUILD_SEARCH_HITS)
//                    .assertTimingPresent(FetchTimingType.NEXT_READER) // May or may not execute depending on segments
//                    .assertAllTimingsNonNegative();
//
//                // Verify fetch results
//                SearchHits hits = context.fetchResult().hits();
//                assertThat(hits.getHits().length, equalTo(docIds.length));
//            }
//        }
//    }
}
