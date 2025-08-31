package org.opensearch.search.query;

import org.opensearch.test.OpenSearchTestCase;
import org.apache.lucene.search.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.document.*;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.search.CollectorManager;
import org.opensearch.search.query.ReduceableSearchResult;

public class StreamingCollectorUnitTests extends OpenSearchTestCase {

    private Directory directory;
    private IndexReader reader;
    private IndexSearcher searcher;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Create in-memory index with test data
        directory = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), directory);

        // Add 100 test documents
        for (int i = 0; i < 100; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", String.valueOf(i), Field.Store.YES));
            doc.add(new TextField("content", "test content " + i, Field.Store.NO));
            doc.add(new NumericDocValuesField("score_field", i));
            writer.addDocument(doc);
        }

        reader = writer.getReader();
        searcher = newSearcher(reader);
        writer.close();
    }

    @Test
    public void testNoScoringCollectorCollectsDocuments() throws Exception {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        StreamingUnsortedCollectorContext context =
            new StreamingUnsortedCollectorContext("test", 10, breaker);

        // NEW PATTERN (WORKING):
        CollectorManager<? extends Collector, ReduceableSearchResult> manager =
            context.createManager(null);
        Collector collector = manager.newCollector();
        searcher.search(new MatchAllDocsQuery(), collector);

        // Use reduce() to get results instead of postProcess()
        @SuppressWarnings("rawtypes")
        List collectors = Collections.singletonList(collector);
        ReduceableSearchResult reduceResult = manager.reduce(collectors);
        QuerySearchResult result = new QuerySearchResult();
        reduceResult.reduce(result);

        assertNotNull("TopDocs should be set", result.topDocs());
        assertEquals("Should collect 10 documents", 10,
                    result.topDocs().topDocs.scoreDocs.length);

        // Verify all scores are NaN for NO_SCORING
        for (ScoreDoc doc : result.topDocs().topDocs.scoreDocs) {
            assertTrue("NO_SCORING should have NaN scores",
                      Float.isNaN(doc.score));
        }
    }

    @Test
    public void testScoredSortedCollectorSortsCorrectly() throws Exception {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        StreamingSortedCollectorContext context =
            new StreamingSortedCollectorContext("test", 10, breaker);

        // NEW PATTERN (WORKING):
        CollectorManager<? extends Collector, ReduceableSearchResult> manager =
            context.createManager(null);
        Collector collector = manager.newCollector();
        searcher.search(new MatchAllDocsQuery(), collector);

        // Use reduce() to get results instead of postProcess()
        @SuppressWarnings("rawtypes")
        List collectors = Collections.singletonList(collector);
        ReduceableSearchResult reduceResult = manager.reduce(collectors);
        QuerySearchResult result = new QuerySearchResult();
        reduceResult.reduce(result);

        assertNotNull("TopDocs should be set", result.topDocs());
        assertTrue("Should have results", result.topDocs().topDocs.scoreDocs.length > 0);

        // Verify scores are in descending order
        ScoreDoc[] docs = result.topDocs().topDocs.scoreDocs;
        for (int i = 1; i < docs.length; i++) {
            assertTrue("Scores should be in descending order",
                      docs[i-1].score >= docs[i].score);
        }
    }

    @Test
    public void testScoredUnsortedCollectorHasScoresButNoSorting() throws Exception {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        StreamingScoredUnsortedCollectorContext context =
            new StreamingScoredUnsortedCollectorContext("test", 10, breaker);

        // NEW PATTERN (WORKING):
        CollectorManager<? extends Collector, ReduceableSearchResult> manager =
            context.createManager(null);
        Collector collector = manager.newCollector();
        searcher.search(new MatchAllDocsQuery(), collector);

        // Use reduce() to get results instead of postProcess()
        @SuppressWarnings("rawtypes")
        List collectors = Collections.singletonList(collector);
        ReduceableSearchResult reduceResult = manager.reduce(collectors);
        QuerySearchResult result = new QuerySearchResult();
        reduceResult.reduce(result);

        assertNotNull("TopDocs should be set", result.topDocs());
        assertTrue("Should have results", result.topDocs().topDocs.scoreDocs.length > 0);

        // Verify scores exist but are not necessarily sorted
        ScoreDoc[] docs = result.topDocs().topDocs.scoreDocs;
        for (ScoreDoc doc : docs) {
            assertFalse("SCORED_UNSORTED should have actual scores, not NaN",
                       Float.isNaN(doc.score));
        }
    }

    @Test
    public void testConfidenceBasedCollectorBasicFunctionality() throws Exception {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        StreamingConfidenceCollectorContext context =
            new StreamingConfidenceCollectorContext("test", 10, breaker);

        // NEW PATTERN (WORKING):
        CollectorManager<? extends Collector, ReduceableSearchResult> manager =
            context.createManager(null);
        Collector collector = manager.newCollector();
        searcher.search(new MatchAllDocsQuery(), collector);

        // Use reduce() to get results instead of postProcess()
        @SuppressWarnings("rawtypes")
        List collectors = Collections.singletonList(collector);
        ReduceableSearchResult reduceResult = manager.reduce(collectors);
        QuerySearchResult result = new QuerySearchResult();
        reduceResult.reduce(result);

        assertNotNull("TopDocs should be set", result.topDocs());
        assertTrue("Should have results", result.topDocs().topDocs.scoreDocs.length > 0);

        // Verify basic functionality works
        ScoreDoc[] docs = result.topDocs().topDocs.scoreDocs;
        assertTrue("Should collect some documents", docs.length > 0);
    }

    @Test
    public void testCollectorManagerReduceLogic() throws Exception {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        StreamingUnsortedCollectorContext context =
            new StreamingUnsortedCollectorContext("test", 5, breaker);

        // Test that we can create a manager
        CollectorManager<? extends Collector, ReduceableSearchResult> manager =
            context.createManager(null);
        assertNotNull("Manager should be created", manager);

        // Test that we can create collectors
        Collector c1 = manager.newCollector();
        Collector c2 = manager.newCollector();
        assertNotNull("Collector 1 should be created", c1);
        assertNotNull("Collector 2 should be created", c2);

        // Test basic collection
        searcher.search(new MatchAllDocsQuery(), c1);
        searcher.search(new MatchAllDocsQuery(), c2);

        // Test reduce functionality
        @SuppressWarnings("rawtypes")
        List collectors = Arrays.asList(c1, c2);
        ReduceableSearchResult reduceResult = manager.reduce(collectors);
        assertNotNull("Reduce result should not be null", reduceResult);

        // Test that we can apply the reduce result
        QuerySearchResult result = new QuerySearchResult();
        reduceResult.reduce(result);
        assertNotNull("TopDocs should be set after reduce", result.topDocs());
    }

    @Test
    public void testAllCollectorModesWork() throws Exception {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        
        // Test all four modes
        StreamingUnsortedCollectorContext noScoringContext =
            new StreamingUnsortedCollectorContext("test", 5, breaker);
        StreamingSortedCollectorContext sortedContext =
            new StreamingSortedCollectorContext("test", 5, breaker);
        StreamingScoredUnsortedCollectorContext unsortedContext =
            new StreamingScoredUnsortedCollectorContext("test", 5, breaker);
        StreamingConfidenceCollectorContext confidenceContext =
            new StreamingConfidenceCollectorContext("test", 5, breaker);

        // Verify all can create managers
        assertNotNull("NO_SCORING manager should be created", noScoringContext.createManager(null));
        assertNotNull("SCORED_SORTED manager should be created", sortedContext.createManager(null));
        assertNotNull("SCORED_UNSORTED manager should be created", unsortedContext.createManager(null));
        assertNotNull("CONFIDENCE_BASED manager should be created", confidenceContext.createManager(null));

        // Test basic functionality for each
        testCollectorMode(noScoringContext, "NO_SCORING");
        testCollectorMode(sortedContext, "SCORED_SORTED");
        testCollectorMode(unsortedContext, "SCORED_UNSORTED");
        testCollectorMode(confidenceContext, "CONFIDENCE_BASED");
    }

    private void testCollectorMode(TopDocsCollectorContext context, String modeName) throws Exception {
        CollectorManager<? extends Collector, ReduceableSearchResult> manager = context.createManager(null);
        Collector collector = manager.newCollector();
        
        searcher.search(new MatchAllDocsQuery(), collector);
        
        @SuppressWarnings("rawtypes")
        List collectors = Collections.singletonList(collector);
        ReduceableSearchResult reduceResult = manager.reduce(collectors);
        QuerySearchResult result = new QuerySearchResult();
        reduceResult.reduce(result);
        
        assertNotNull(modeName + " should set TopDocs", result.topDocs());
        assertTrue(modeName + " should have results", result.topDocs().topDocs.scoreDocs.length > 0);
    }

    @After
    public void tearDown() throws Exception {
        if (reader != null) {
            reader.close();
        }
        if (directory != null) {
            directory.close();
        }
        super.tearDown();
    }
}
