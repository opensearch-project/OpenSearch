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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.core.common.text.Text;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.ParsedQuery;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.subphase.ExplainPhase;
import org.opensearch.search.fetch.subphase.FetchDocValuesContext;
import org.opensearch.search.fetch.subphase.FetchDocValuesPhase;
import org.opensearch.search.fetch.subphase.FetchFieldsContext;
import org.opensearch.search.fetch.subphase.FetchFieldsPhase;
import org.opensearch.search.fetch.subphase.FetchScorePhase;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.FetchSourcePhase;
import org.opensearch.search.fetch.subphase.FetchVersionPhase;
import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.search.fetch.subphase.InnerHitsContext;
import org.opensearch.search.fetch.subphase.MatchedQueriesPhase;
import org.opensearch.search.fetch.subphase.ScriptFieldsContext;
import org.opensearch.search.fetch.subphase.SeqNoPrimaryTermPhase;
import org.opensearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.search.fetch.subphase.highlight.HighlightPhase;
import org.opensearch.search.fetch.subphase.highlight.Highlighter;
import org.opensearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.opensearch.search.internal.ContextIndexSearcher;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.SubSearchContext;
import org.opensearch.search.lookup.SearchLookup;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.search.profile.ProfileResult;
import org.opensearch.search.profile.Profilers;
import org.opensearch.search.profile.fetch.FetchProfiler;
import org.opensearch.search.profile.fetch.FetchTimingType;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
        private ScriptFieldsContext scriptFieldsContext = null;

        private boolean enableExplain = false;
        private boolean enableVersion = false;
        private boolean enableSeqNoPrimaryTerm = false;
        private boolean enableFetchScore = false;

        private boolean enableDocValues = false;
        private List<FieldAndFormat> docValueFields = Collections.emptyList();

        private boolean enableFetchFields = false;
        private List<FieldAndFormat> fetchFields = Collections.emptyList();

        private ParsedQuery parsedQuery = null;
        private Query query = new MatchAllDocsQuery();

        private SearchHighlightContext highlightContext = null;
        private QueryShardContext externalQueryShardContext = null;

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

        SearchContextBuilder withScriptFields(ScriptFieldsContext ctx) {
            this.enableScriptFields = true;
            this.scriptFieldsContext = ctx;
            return this;
        }

        SearchContextBuilder withExplain() {
            this.enableExplain = true;
            return this;
        }

        SearchContextBuilder withVersion() {
            this.enableVersion = true;
            return this;
        }

        SearchContextBuilder withSeqNoPrimaryTerm() {
            this.enableSeqNoPrimaryTerm = true;
            return this;
        }

        SearchContextBuilder withFetchScore() {
            this.enableFetchScore = true;
            return this;
        }

        SearchContextBuilder withDocValues(FieldAndFormat... fields) {
            this.enableDocValues = true;
            this.docValueFields = Arrays.asList(fields);
            return this;
        }

        SearchContextBuilder withFetchFields(FieldAndFormat... fields) {
            this.enableFetchFields = true;
            this.fetchFields = Arrays.asList(fields);
            return this;
        }

        SearchContextBuilder withParsedQuery(ParsedQuery pq) {
            this.parsedQuery = pq;
            return this;
        }

        SearchContextBuilder withQuery(Query query) {
            this.query = query;
            return this;
        }

        SearchContextBuilder withHighlight(SearchHighlightContext hc) {
            this.highlightContext = hc;
            return this;
        }

        SearchContextBuilder withQueryShardContext(QueryShardContext qsc) {
            this.externalQueryShardContext = qsc;
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

            if (enableScriptFields) {
                when(context.scriptFields()).thenReturn(scriptFieldsContext);
            }

            // Basic query configuration
            when(context.query()).thenReturn(query);
            when(context.parsedQuery()).thenReturn(parsedQuery);
            when(context.explain()).thenReturn(enableExplain);
            when(context.version()).thenReturn(enableVersion);
            when(context.seqNoAndPrimaryTerm()).thenReturn(enableSeqNoPrimaryTerm);

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

            if (enableDocValues) {
                when(context.docValuesContext()).thenReturn(new FetchDocValuesContext(docValueFields));
            } else {
                when(context.docValuesContext()).thenReturn(null);
            }

            if (enableFetchFields) {
                when(context.fetchFieldsContext()).thenReturn(new FetchFieldsContext(fetchFields));
            } else {
                when(context.fetchFieldsContext()).thenReturn(null);
            }

            if (highlightContext != null) {
                when(context.highlight()).thenReturn(highlightContext);
            } else {
                when(context.highlight()).thenReturn(null);
            }

            if (enableFetchScore) {
                Sort sort = new Sort(SortField.FIELD_SCORE);
                SortAndFormats sf = new SortAndFormats(sort, new DocValueFormat[] { DocValueFormat.RAW });
                when(context.sort()).thenReturn(sf);
                when(context.trackScores()).thenReturn(true);
            } else {
                when(context.sort()).thenReturn(null);
                when(context.trackScores()).thenReturn(false);
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
            QueryShardContext queryShardContext = externalQueryShardContext != null
                ? externalQueryShardContext
                : mock(QueryShardContext.class);
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

            // Mock the sourceMapper
            org.opensearch.index.mapper.SourceFieldMapper sourceFieldMapper = mock(org.opensearch.index.mapper.SourceFieldMapper.class);
            when(sourceFieldMapper.enabled()).thenReturn(true);
            when(documentMapper.sourceMapper()).thenReturn(sourceFieldMapper);

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

    // Simple highlighter
    private static class StubHighlighter implements Highlighter {
        @Override
        public HighlightField highlight(FieldHighlightContext fieldContext) {
            return null;
        }

        @Override
        public boolean canHighlight(MappedFieldType fieldType) {
            return true;
        }
    }

    // Minimal inner hit sub context
    private static class DummyInnerHitSubContext extends InnerHitsContext.InnerHitSubContext {
        DummyInnerHitSubContext(String name, SearchContext context) {
            super(name, context);
        }

        @Override
        public TopDocsAndMaxScore topDocs(SearchHit hit) {
            ScoreDoc[] docs = new ScoreDoc[] { new ScoreDoc(hit.docId(), 1.0f) };
            TopDocs topDocs = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), docs);
            return new TopDocsAndMaxScore(topDocs, 1.0f);
        }
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
                    .assertTimingPresent(FetchTimingType.BUILD_SUB_PHASE_PROCESSORS)
                    .assertTimingPresent(FetchTimingType.GET_NEXT_READER);
            }
        }
    }

    public void testExplainPhaseProfiling() throws Exception {
        try (Directory dir = newDirectory()) {
            List<Document> docs = new TestDocumentBuilder().addDocuments(1, true).build();
            int[] docIds = indexDocumentsAndGetIds(dir, docs, 1);

            try (IndexReader reader = DirectoryReader.open(dir)) {
                QueryShardContext qsc = mock(QueryShardContext.class);
                ParsedQuery pq = new ParsedQuery(new MatchAllDocsQuery(), Map.of("named", new MatchAllDocsQuery()));

                // Create a search context with only explain enabled
                SearchContext context = new SearchContextBuilder(reader, docIds, indexShard).withSourceLoading()
                    .withStoredFields("_source")
                    .withQueryShardContext(qsc)
                    .withExplain() // Only enable explain
                    .withParsedQuery(pq)
                    .build();

                // Only include the ExplainPhase in the sub-phases list
                List<FetchSubPhase> subPhases = Collections.singletonList(new ExplainPhase());

                ProfileResult profile = executeFetchPhaseAndGetProfile(context, subPhases);

                // Verify the profile results
                Map<String, ProfileResult> children = new HashMap<>();
                for (ProfileResult child : profile.getProfiledChildren()) {
                    children.put(child.getQueryName(), child);
                }

                // Check that ExplainPhase is present and has timing information
                assertEquals(1, children.size());
                assertTrue("Missing profile for ExplainPhase", children.containsKey("ExplainPhase"));
                new TimingAssertions(children.get("ExplainPhase").getTimeBreakdown()).assertTimingPresent(FetchTimingType.PROCESS)
                    .assertTimingPresent(FetchTimingType.SET_NEXT_READER);
            }
        }
    }

    public void testFetchSourcePhaseProfiling() throws Exception {
        try (Directory dir = newDirectory()) {
            List<Document> docs = new TestDocumentBuilder().addDocuments(1, true).build();
            int[] docIds = indexDocumentsAndGetIds(dir, docs, 1);

            try (IndexReader reader = DirectoryReader.open(dir)) {
                QueryShardContext qsc = mock(QueryShardContext.class);
                ParsedQuery pq = new ParsedQuery(new MatchAllDocsQuery());

                // Create a search context with only source loading enabled
                SearchContext context = new SearchContextBuilder(reader, docIds, indexShard).withSourceLoading()  // Enable source loading
                    .withStoredFields("_source")
                    .withQueryShardContext(qsc)
                    .withParsedQuery(pq)
                    .build();

                // Only include the FetchSourcePhase in the sub-phases list
                List<FetchSubPhase> subPhases = Collections.singletonList(new FetchSourcePhase());

                ProfileResult profile = executeFetchPhaseAndGetProfile(context, subPhases);

                // Verify the profile results
                Map<String, ProfileResult> children = new HashMap<>();
                for (ProfileResult child : profile.getProfiledChildren()) {
                    children.put(child.getQueryName(), child);
                }

                // Check that FetchSourcePhase is present and has timing information
                assertEquals(1, children.size());
                assertTrue("Missing profile for FetchSourcePhase", children.containsKey("FetchSourcePhase"));
                new TimingAssertions(children.get("FetchSourcePhase").getTimeBreakdown()).assertTimingPresent(FetchTimingType.PROCESS)
                    .assertTimingPresent(FetchTimingType.SET_NEXT_READER);
            }
        }
    }

    public void testFetchDocValuesPhaseProfiling() throws Exception {
        try (Directory dir = newDirectory()) {
            List<Document> docs = new TestDocumentBuilder().addDocuments(1, true).build();
            int[] docIds = indexDocumentsAndGetIds(dir, docs, 1);

            try (IndexReader reader = DirectoryReader.open(dir)) {
                QueryShardContext qsc = mock(QueryShardContext.class);
                ParsedQuery pq = new ParsedQuery(new MatchAllDocsQuery());

                // Create a search context with doc values enabled
                SearchContext context = new SearchContextBuilder(reader, docIds, indexShard).withSourceLoading()
                    .withStoredFields("_source")
                    .withQueryShardContext(qsc)
                    .withParsedQuery(pq)
                    .withDocValues(new FieldAndFormat("id", null))  // Enable doc values for the id field
                    .build();

                // Only include the FetchDocValuesPhase in the sub-phases list
                List<FetchSubPhase> subPhases = Collections.singletonList(new FetchDocValuesPhase());

                ProfileResult profile = executeFetchPhaseAndGetProfile(context, subPhases);

                // Verify the profile results
                Map<String, ProfileResult> children = new HashMap<>();
                for (ProfileResult child : profile.getProfiledChildren()) {
                    children.put(child.getQueryName(), child);
                }

                // Check that FetchDocValuesPhase is present and has timing information
                assertEquals(1, children.size());
                assertTrue("Missing profile for FetchDocValuesPhase", children.containsKey("FetchDocValuesPhase"));
                new TimingAssertions(children.get("FetchDocValuesPhase").getTimeBreakdown()).assertTimingPresent(FetchTimingType.PROCESS)
                    .assertTimingPresent(FetchTimingType.SET_NEXT_READER);
            }
        }
    }

    public void testFetchFieldsPhaseProfiling() throws Exception {
        try (Directory dir = newDirectory()) {
            List<Document> docs = new TestDocumentBuilder().addDocuments(1, true).build();
            int[] docIds = indexDocumentsAndGetIds(dir, docs, 1);

            try (IndexReader reader = DirectoryReader.open(dir)) {
                QueryShardContext qsc = mock(QueryShardContext.class);
                ParsedQuery pq = new ParsedQuery(new MatchAllDocsQuery());

                // Create a search context with fetch fields enabled
                SearchContext context = new SearchContextBuilder(reader, docIds, indexShard).withSourceLoading()
                    .withStoredFields("_source")
                    .withQueryShardContext(qsc)
                    .withParsedQuery(pq)
                    .withFetchFields(new FieldAndFormat("content", null))  // Enable fetch fields for the content field
                    .build();

                // Only include the FetchFieldsPhase in the sub-phases list
                List<FetchSubPhase> subPhases = Collections.singletonList(new FetchFieldsPhase());

                ProfileResult profile = executeFetchPhaseAndGetProfile(context, subPhases);

                // Verify the profile results
                Map<String, ProfileResult> children = new HashMap<>();
                for (ProfileResult child : profile.getProfiledChildren()) {
                    children.put(child.getQueryName(), child);
                }

                // Check that FetchFieldsPhase is present and has timing information
                assertEquals(1, children.size());
                assertTrue("Missing profile for FetchFieldsPhase", children.containsKey("FetchFieldsPhase"));
                new TimingAssertions(children.get("FetchFieldsPhase").getTimeBreakdown()).assertTimingPresent(FetchTimingType.PROCESS)
                    .assertTimingPresent(FetchTimingType.SET_NEXT_READER);
            }
        }
    }

    public void testFetchVersionPhaseProfiling() throws Exception {
        try (Directory dir = newDirectory()) {
            List<Document> docs = new TestDocumentBuilder().addDocuments(1, true).build();
            int[] docIds = indexDocumentsAndGetIds(dir, docs, 1);

            try (IndexReader reader = DirectoryReader.open(dir)) {
                QueryShardContext qsc = mock(QueryShardContext.class);
                ParsedQuery pq = new ParsedQuery(new MatchAllDocsQuery());

                // Create a search context with version enabled
                SearchContext context = new SearchContextBuilder(reader, docIds, indexShard).withSourceLoading()
                    .withStoredFields("_source")
                    .withQueryShardContext(qsc)
                    .withParsedQuery(pq)
                    .withVersion() // Enable version
                    .build();

                // Only include the FetchVersionPhase in the sub-phases list
                List<FetchSubPhase> subPhases = Collections.singletonList(new FetchVersionPhase());

                ProfileResult profile = executeFetchPhaseAndGetProfile(context, subPhases);

                // Verify the profile results
                Map<String, ProfileResult> children = new HashMap<>();
                for (ProfileResult child : profile.getProfiledChildren()) {
                    children.put(child.getQueryName(), child);
                }

                // Check that FetchVersionPhase is present and has timing information
                assertEquals(1, children.size());
                assertTrue("Missing profile for FetchVersionPhase", children.containsKey("FetchVersionPhase"));
                new TimingAssertions(children.get("FetchVersionPhase").getTimeBreakdown()).assertTimingPresent(FetchTimingType.PROCESS)
                    .assertTimingPresent(FetchTimingType.SET_NEXT_READER);
            }
        }
    }

    public void testSeqNoPrimaryTermPhaseProfiling() throws Exception {
        try (Directory dir = newDirectory()) {
            List<Document> docs = new TestDocumentBuilder().addDocuments(1, true).build();
            int[] docIds = indexDocumentsAndGetIds(dir, docs, 1);

            try (IndexReader reader = DirectoryReader.open(dir)) {
                QueryShardContext qsc = mock(QueryShardContext.class);
                ParsedQuery pq = new ParsedQuery(new MatchAllDocsQuery());

                // Create a search context with sequence number and primary term enabled
                SearchContext context = new SearchContextBuilder(reader, docIds, indexShard).withSourceLoading()
                    .withStoredFields("_source")
                    .withQueryShardContext(qsc)
                    .withParsedQuery(pq)
                    .withSeqNoPrimaryTerm() // Enable sequence number and primary term
                    .build();

                // Only include the SeqNoPrimaryTermPhase in the sub-phases list
                List<FetchSubPhase> subPhases = Collections.singletonList(new SeqNoPrimaryTermPhase());

                ProfileResult profile = executeFetchPhaseAndGetProfile(context, subPhases);

                // Verify the profile results
                Map<String, ProfileResult> children = new HashMap<>();
                for (ProfileResult child : profile.getProfiledChildren()) {
                    children.put(child.getQueryName(), child);
                }

                // Check that SeqNoPrimaryTermPhase is present and has timing information
                assertEquals(1, children.size());
                assertTrue("Missing profile for SeqNoPrimaryTermPhase", children.containsKey("SeqNoPrimaryTermPhase"));
                new TimingAssertions(children.get("SeqNoPrimaryTermPhase").getTimeBreakdown()).assertTimingPresent(FetchTimingType.PROCESS)
                    .assertTimingPresent(FetchTimingType.SET_NEXT_READER);
            }
        }
    }

    public void testMatchedQueriesPhaseProfiling() throws Exception {
        try (Directory dir = newDirectory()) {
            List<Document> docs = new TestDocumentBuilder().addDocuments(1, true).build();
            int[] docIds = indexDocumentsAndGetIds(dir, docs, 1);

            try (IndexReader reader = DirectoryReader.open(dir)) {
                QueryShardContext qsc = mock(QueryShardContext.class);

                // Create a parsed query with named queries for the MatchedQueriesPhase to extract
                ParsedQuery pq = new ParsedQuery(new MatchAllDocsQuery(), Map.of("test_query", new MatchAllDocsQuery()));

                // Create a search context with the parsed query containing named queries
                SearchContext context = new SearchContextBuilder(reader, docIds, indexShard).withSourceLoading()
                    .withStoredFields("_source")
                    .withQueryShardContext(qsc)
                    .withParsedQuery(pq)
                    .build();

                // Only include the MatchedQueriesPhase in the sub-phases list
                List<FetchSubPhase> subPhases = Collections.singletonList(new MatchedQueriesPhase());

                ProfileResult profile = executeFetchPhaseAndGetProfile(context, subPhases);

                // Verify the profile results
                Map<String, ProfileResult> children = new HashMap<>();
                for (ProfileResult child : profile.getProfiledChildren()) {
                    children.put(child.getQueryName(), child);
                }

                // Check that MatchedQueriesPhase is present and has timing information
                assertEquals(1, children.size());
                assertTrue("Missing profile for MatchedQueriesPhase", children.containsKey("MatchedQueriesPhase"));
                new TimingAssertions(children.get("MatchedQueriesPhase").getTimeBreakdown()).assertTimingPresent(FetchTimingType.PROCESS)
                    .assertTimingPresent(FetchTimingType.SET_NEXT_READER);
            }
        }
    }

    public void testHighlightPhaseProfiling() throws Exception {
        try (Directory dir = newDirectory()) {
            List<Document> docs = new TestDocumentBuilder().addDocuments(1, true).build();
            int[] docIds = indexDocumentsAndGetIds(dir, docs, 1);

            try (IndexReader reader = DirectoryReader.open(dir)) {
                QueryShardContext qsc = mock(QueryShardContext.class);
                ParsedQuery pq = new ParsedQuery(new MatchAllDocsQuery());

                // Create highlight builder with stub highlighter
                HighlightBuilder hb = new HighlightBuilder();
                hb.field(new HighlightBuilder.Field("content").highlighterType("stub"));
                SearchHighlightContext highlight = hb.build(qsc);

                // Create a search context with highlighting enabled
                SearchContext context = new SearchContextBuilder(reader, docIds, indexShard).withSourceLoading()
                    .withStoredFields("_source")
                    .withQueryShardContext(qsc)
                    .withParsedQuery(pq)
                    .withHighlight(highlight)
                    .build();

                // Only include the HighlightPhase in the sub-phases list with the stub highlighter
                List<FetchSubPhase> subPhases = Collections.singletonList(
                    new HighlightPhase(Collections.singletonMap("stub", new StubHighlighter()))
                );

                ProfileResult profile = executeFetchPhaseAndGetProfile(context, subPhases);

                // Verify the profile results
                Map<String, ProfileResult> children = new HashMap<>();
                for (ProfileResult child : profile.getProfiledChildren()) {
                    children.put(child.getQueryName(), child);
                }

                // Check that HighlightPhase is present and has timing information
                assertEquals(1, children.size());
                assertTrue("Missing profile for HighlightPhase", children.containsKey("HighlightPhase"));
                new TimingAssertions(children.get("HighlightPhase").getTimeBreakdown()).assertTimingPresent(FetchTimingType.PROCESS)
                    .assertTimingPresent(FetchTimingType.SET_NEXT_READER);
            }
        }
    }

    public void testFetchScorePhaseProfiling() throws Exception {
        try (Directory dir = newDirectory()) {
            List<Document> docs = new TestDocumentBuilder().addDocuments(1, true).build();
            int[] docIds = indexDocumentsAndGetIds(dir, docs, 1);

            try (IndexReader reader = DirectoryReader.open(dir)) {
                QueryShardContext qsc = mock(QueryShardContext.class);
                ParsedQuery pq = new ParsedQuery(new MatchAllDocsQuery());

                // Create a search context with score tracking enabled
                SearchContext context = new SearchContextBuilder(reader, docIds, indexShard).withSourceLoading()
                    .withStoredFields("_source")
                    .withQueryShardContext(qsc)
                    .withParsedQuery(pq)
                    .withFetchScore() // Enable score fetching
                    .build();

                // Only include the FetchScorePhase in the sub-phases list
                List<FetchSubPhase> subPhases = Collections.singletonList(new FetchScorePhase());

                ProfileResult profile = executeFetchPhaseAndGetProfile(context, subPhases);

                // Verify the profile results
                Map<String, ProfileResult> children = new HashMap<>();
                for (ProfileResult child : profile.getProfiledChildren()) {
                    children.put(child.getQueryName(), child);
                }

                // Check that FetchScorePhase is present and has timing information
                assertEquals(1, children.size());
                assertTrue("Missing profile for FetchScorePhase", children.containsKey("FetchScorePhase"));
                new TimingAssertions(children.get("FetchScorePhase").getTimeBreakdown()).assertTimingPresent(FetchTimingType.PROCESS)
                    .assertTimingPresent(FetchTimingType.SET_NEXT_READER);
            }
        }
    }

    public void testInnerHitsPhaseProfiling() throws Exception {
        try (Directory dir = newDirectory()) {
            List<Document> docs = new TestDocumentBuilder().addDocuments(1, true).build();
            int[] docIds = indexDocumentsAndGetIds(dir, docs, 1);

            try (IndexReader reader = DirectoryReader.open(dir)) {
                QueryShardContext qsc = mock(QueryShardContext.class);
                ParsedQuery pq = new ParsedQuery(new MatchAllDocsQuery());

                SearchContext context = new SearchContextBuilder(reader, docIds, indexShard).withSourceLoading()
                    .withStoredFields("_source")
                    .withQueryShardContext(qsc)
                    .withParsedQuery(pq)
                    .build();

                // Create multiple inner hit contexts
                InnerHitsContext.InnerHitSubContext innerContext1 = new DummyInnerHitSubContext("inner1", context);
                InnerHitsContext.InnerHitSubContext innerContext2 = new DummyInnerHitSubContext("inner2", context);
                InnerHitsContext.InnerHitSubContext innerContext3 = new DummyInnerHitSubContext("inner3", context);

                InnerHitsContext innerHits = new InnerHitsContext();
                innerHits.addInnerHitDefinition(innerContext1);
                innerHits.addInnerHitDefinition(innerContext2);
                innerHits.addInnerHitDefinition(innerContext3);
                when(context.innerHits()).thenReturn(innerHits);

                List<FetchSubPhase> subPhases = Collections.singletonList(new FetchSourcePhase());

                FetchPhase fetchPhase = new FetchPhase(subPhases);
                fetchPhase.execute(context);

                FetchProfiler fetchProfiler = context.getProfilers().getFetchProfiler();
                List<ProfileResult> profileResults = fetchProfiler.getTree();

                // Should have 1 standard fetch profile + 3 inner hits fetch profiles = 4 total
                assertThat(profileResults, hasSize(4));

                // Verify the standard fetch profile (first result)
                ProfileResult standardFetchProfile = profileResults.get(0);

                Map<String, ProfileResult> children = new HashMap<>();
                for (ProfileResult child : standardFetchProfile.getProfiledChildren()) {
                    children.put(child.getQueryName(), child);
                }

                assertEquals(1, children.size());
                assertFalse(children.containsKey("InnerHitsPhase"));
                assertTrue(children.containsKey("FetchSourcePhase"));

                new TimingAssertions(children.get("FetchSourcePhase").getTimeBreakdown()).assertTimingPresent(FetchTimingType.PROCESS)
                    .assertTimingPresent(FetchTimingType.SET_NEXT_READER);

                Set<String> expectedInnerHitNames = Set.of("inner1", "inner2", "inner3");
                Set<String> actualInnerHitNames = new HashSet<>();
                List<ProfileResult> innerHitsProfiles = new ArrayList<>();

                for (int i = 1; i < profileResults.size(); i++) {
                    ProfileResult profile = profileResults.get(i);
                    String profileName = profile.getQueryName();

                    assertTrue("Profile name should start with 'fetch_inner_hits['", profileName.startsWith("fetch_inner_hits["));
                    assertTrue("Profile name should end with ']'", profileName.endsWith("]"));

                    String innerHitName = profileName.substring("fetch_inner_hits[".length(), profileName.length() - 1);
                    actualInnerHitNames.add(innerHitName);
                    innerHitsProfiles.add(profile);
                }

                assertEquals("Should have all expected inner hit names", expectedInnerHitNames, actualInnerHitNames);
                assertEquals("Should have 3 inner hits profiles", 3, innerHitsProfiles.size());

                for (ProfileResult innerHitsFetchProfile : innerHitsProfiles) {
                    new TimingAssertions(innerHitsFetchProfile.getTimeBreakdown()).assertBreakdownNotEmpty()
                        .assertTimingPresent(FetchTimingType.CREATE_STORED_FIELDS_VISITOR)
                        .assertTimingPresent(FetchTimingType.LOAD_SOURCE)
                        .assertTimingPresent(FetchTimingType.LOAD_STORED_FIELDS)
                        .assertTimingPresent(FetchTimingType.BUILD_SUB_PHASE_PROCESSORS)
                        .assertTimingPresent(FetchTimingType.GET_NEXT_READER);

                    children = new HashMap<>();
                    for (ProfileResult child : innerHitsFetchProfile.getProfiledChildren()) {
                        children.put(child.getQueryName(), child);
                    }

                    String innerHitName = innerHitsFetchProfile.getQueryName();
                    assertEquals("Inner hit " + innerHitName + " should have 1 sub-phase", 1, children.size());
                    assertTrue("Inner hit " + innerHitName + " should contain FetchSourcePhase", children.containsKey("FetchSourcePhase"));

                    new TimingAssertions(children.get("FetchSourcePhase").getTimeBreakdown()).assertTimingPresent(FetchTimingType.PROCESS)
                        .assertTimingPresent(FetchTimingType.SET_NEXT_READER);
                }
            }
        }
    }

    public void testTopHitsAggregationFetchProfiling() throws Exception {
        try (Directory dir = newDirectory()) {
            List<Document> docs = new TestDocumentBuilder().addDocuments(3, true).build();
            int[] docIds = indexDocumentsAndGetIds(dir, docs, 3);

            try (IndexReader reader = DirectoryReader.open(dir)) {
                QueryShardContext qsc = mock(QueryShardContext.class);
                ParsedQuery pq = new ParsedQuery(new MatchAllDocsQuery());

                // Create the main search context
                SearchContext mainContext = new SearchContextBuilder(reader, docIds, indexShard).withSourceLoading()
                    .withStoredFields("_source")
                    .withQueryShardContext(qsc)
                    .withParsedQuery(pq)
                    .build();

                // Create multiple SubSearchContext instances to simulate top hits aggregations
                SubSearchContext topHitsContext1 = new SubSearchContext(mainContext);
                topHitsContext1.docIdsToLoad(new int[] { docIds[0] }, 0, 1);
                topHitsContext1.size(1);

                SubSearchContext topHitsContext2 = new SubSearchContext(mainContext);
                topHitsContext2.docIdsToLoad(new int[] { docIds[1] }, 0, 1);
                topHitsContext2.size(1);

                SubSearchContext topHitsContext3 = new SubSearchContext(mainContext);
                topHitsContext3.docIdsToLoad(new int[] { docIds[2] }, 0, 1);
                topHitsContext3.size(1);

                List<FetchSubPhase> subPhases = Collections.singletonList(new FetchSourcePhase());
                FetchPhase fetchPhase = new FetchPhase(subPhases);

                fetchPhase.execute(topHitsContext1, "fetch_top_hits_aggregation[top_hits_agg1]");
                fetchPhase.execute(topHitsContext2, "fetch_top_hits_aggregation[top_hits_agg2]");
                fetchPhase.execute(topHitsContext3, "fetch_top_hits_aggregation[top_hits_agg3]");

                FetchProfiler fetchProfiler = mainContext.getProfilers().getFetchProfiler();
                List<ProfileResult> profileResults = fetchProfiler.getTree();

                assertThat(profileResults, hasSize(3));

                Set<String> expectedTopHitsNames = Set.of("top_hits_agg1", "top_hits_agg2", "top_hits_agg3");
                Set<String> actualTopHitsNames = new HashSet<>();

                for (ProfileResult profile : profileResults) {
                    String profileName = profile.getQueryName();

                    assertTrue(
                        "Profile name should start with 'fetch_top_hits_aggregation['",
                        profileName.startsWith("fetch_top_hits_aggregation[")
                    );
                    assertTrue("Profile name should end with ']'", profileName.endsWith("]"));

                    String topHitsName = profileName.substring("fetch_top_hits_aggregation[".length(), profileName.length() - 1);
                    actualTopHitsNames.add(topHitsName);
                }

                assertEquals("Should have all expected top hits names", expectedTopHitsNames, actualTopHitsNames);

                for (ProfileResult topHitsFetchProfile : profileResults) {
                    new TimingAssertions(topHitsFetchProfile.getTimeBreakdown()).assertBreakdownNotEmpty()
                        .assertTimingPresent(FetchTimingType.CREATE_STORED_FIELDS_VISITOR)
                        .assertTimingPresent(FetchTimingType.LOAD_SOURCE)
                        .assertTimingPresent(FetchTimingType.LOAD_STORED_FIELDS)
                        .assertTimingPresent(FetchTimingType.BUILD_SUB_PHASE_PROCESSORS)
                        .assertTimingPresent(FetchTimingType.GET_NEXT_READER);

                    Map<String, ProfileResult> children = new HashMap<>();
                    for (ProfileResult child : topHitsFetchProfile.getProfiledChildren()) {
                        children.put(child.getQueryName(), child);
                    }

                    String topHitsName = topHitsFetchProfile.getQueryName();
                    assertEquals("Top hits " + topHitsName + " should have 1 sub-phase", 1, children.size());
                    assertTrue("Top hits " + topHitsName + " should contain FetchSourcePhase", children.containsKey("FetchSourcePhase"));

                    new TimingAssertions(children.get("FetchSourcePhase").getTimeBreakdown()).assertTimingPresent(FetchTimingType.PROCESS)
                        .assertTimingPresent(FetchTimingType.SET_NEXT_READER);
                }
            }
        }
    }
}
