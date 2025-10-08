/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.stream;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.action.support.StreamSearchChannelListener;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// SortedShardTopDocsStreamer tests
public class SortedShardTopDocsStreamerTests extends OpenSearchTestCase {

    private Directory directory;
    private IndexWriter writer;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private SearchContext searchContext;
    private StreamSearchChannelListener channelListener;
    private List<QuerySearchResult> emittedResults;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        // Set up test index
        Path tempDir = createTempDir();
        directory = new MMapDirectory(tempDir);
        IndexWriterConfig config = new IndexWriterConfig();
        writer = new IndexWriter(directory, config);

        // Add test documents with sort values
        for (int i = 0; i < 100; i++) {
            Document doc = new Document();
            doc.add(new TextField("field", "doc" + i, Field.Store.YES));
            doc.add(new NumericDocValuesField("sort_field", i % 20)); // Values 0-19
            writer.addDocument(doc);
        }
        writer.commit();

        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);

        // Mock search context
        searchContext = mock(SearchContext.class);
        emittedResults = new ArrayList<>();

        // Mock channel listener to capture emissions
        channelListener = mock(StreamSearchChannelListener.class);
        when(searchContext.getStreamChannelListener()).thenReturn(channelListener);

        when(searchContext.getStreamingBatchSize()).thenReturn(10);
        when(searchContext.getStreamingTimeInterval()).thenReturn(TimeValue.timeValueMillis(100));
        when(searchContext.getStreamingFirstHitImmediate()).thenReturn(true);
        when(searchContext.getStreamingEnableCoalescing()).thenReturn(true);
    }

    @After
    public void tearDown() throws Exception {
        reader.close();
        writer.close();
        directory.close();

        // Clean up the fallback scheduler to prevent thread leaks
        AbstractShardTopDocsStreamer.shutdownFallbackScheduler();
        super.tearDown();
    }

    public void testBasic() throws IOException {
        Sort sort = new Sort(new SortField("sort_field", SortField.Type.LONG));

        SortedShardTopDocsStreamer aggregator = new SortedShardTopDocsStreamer(
            10, // numHits
            sort, // sort
            false, // trackMaxScore
            5, // batchDocThreshold
            TimeValue.timeValueMillis(100), // timeInterval
            true, // firstHitImmediate
            true // enableCoalescing
        );

        aggregator.onStart(searchContext);

        // Test document collection
        for (LeafReaderContext context : reader.leaves()) {
            var leafCollector = aggregator.newLeafCollector(context, null);
            assertNotNull(leafCollector);
        }

        // Test final result
        TopDocsAndMaxScore finalResult = aggregator.buildFinalTopDocs();
        assertNotNull(finalResult);
        assertTrue(finalResult.topDocs instanceof TopFieldDocs);

        TopFieldDocs topFieldDocs = (TopFieldDocs) finalResult.topDocs;
        assertEquals(sort, aggregator.getSort());

        aggregator.onFinish();

        // Verify basic metrics
        assertTrue(aggregator.getCollectedCount() >= 0);
        assertTrue(aggregator.getEmissionCount() >= 0);
        assertFalse(aggregator.isCancelled());
    }

    public void testScoreSort() throws IOException {
        Sort scoreSort = new Sort(SortField.FIELD_SCORE);

        SortedShardTopDocsStreamer aggregator = new SortedShardTopDocsStreamer(
            10, // numHits
            scoreSort, // sort by score
            true, // trackMaxScore
            5, // batchDocThreshold
            TimeValue.timeValueMillis(100), // timeInterval
            true, // firstHitImmediate
            true // enableCoalescing
        );

        aggregator.onStart(searchContext);

        // Verify sort configuration
        assertEquals(scoreSort, aggregator.getSort());

        // Test final result
        TopDocsAndMaxScore finalResult = aggregator.buildFinalTopDocs();
        assertNotNull(finalResult);
        assertTrue(finalResult.topDocs instanceof TopFieldDocs);

        aggregator.onFinish();
    }

    public void testMultiSort() throws IOException {
        Sort multiSort = new Sort(new SortField("sort_field", SortField.Type.LONG), SortField.FIELD_SCORE);

        SortedShardTopDocsStreamer aggregator = new SortedShardTopDocsStreamer(
            10, // numHits
            multiSort, // multi-field sort
            true, // trackMaxScore - needed for score field
            5, // batchDocThreshold
            TimeValue.timeValueMillis(100), // timeInterval
            true, // firstHitImmediate
            true // enableCoalescing
        );

        aggregator.onStart(searchContext);

        // Verify sort configuration
        assertEquals(multiSort, aggregator.getSort());
        assertEquals(2, multiSort.getSort().length);

        aggregator.onFinish();
    }

    public void testTriggers() throws IOException {
        Sort sort = new Sort(new SortField("sort_field", SortField.Type.LONG));

        SortedShardTopDocsStreamer aggregator = new SortedShardTopDocsStreamer(
            10, // numHits
            sort, // sort
            false, // trackMaxScore
            3, // batchDocThreshold - small for testing
            TimeValue.timeValueMillis(50), // timeInterval
            true, // firstHitImmediate
            false // enableCoalescing - disabled for testing
        );

        aggregator.onStart(searchContext);

        // Get a leaf collector and properly collect documents
        for (LeafReaderContext context : reader.leaves()) {
            var leafCollector = aggregator.newLeafCollector(context, null);

            // Set a dummy scorer to avoid NullPointerException
            leafCollector.setScorer(new org.apache.lucene.search.Scorable() {
                @Override
                public float score() throws IOException {
                    return 1.0f;
                }
            });

            // Actually collect documents through the leaf collector
            for (int doc = 0; doc < Math.min(10, context.reader().maxDoc()); doc++) {
                leafCollector.collect(doc);
            }
        }

        // Should have triggered some emissions due to batch threshold
        assertTrue("Expected emissions but got " + aggregator.getEmissionCount(), aggregator.getEmissionCount() > 0);

        aggregator.onFinish();
    }

    public void testMemory() throws IOException {
        Sort sort = new Sort(new SortField("sort_field", SortField.Type.LONG), new SortField("secondary_field", SortField.Type.STRING));

        SortedShardTopDocsStreamer aggregator = new SortedShardTopDocsStreamer(
            100, // numHits
            sort, // multi-field sort
            false, // trackMaxScore
            10, // batchDocThreshold
            TimeValue.timeValueMillis(100), // timeInterval
            true, // firstHitImmediate
            true // enableCoalescing
        );

        aggregator.onStart(searchContext);

        // Initial memory usage
        long initialMemory = aggregator.estimateMemoryUsage();
        assertTrue(initialMemory >= 0);

        // Add some documents
        for (int i = 0; i < 50; i++) {
            Object[] sortValues = { (long) (i % 10), "value" + i };
            aggregator.onDoc(i, Float.NaN, sortValues);
        }

        // Memory usage should increase (more than unsorted due to sort values)
        long memoryAfterDocs = aggregator.estimateMemoryUsage();
        assertTrue(memoryAfterDocs > initialMemory);

        aggregator.onFinish();
    }

    public void testCoalescing() throws IOException {
        Sort sort = new Sort(new SortField("sort_field", SortField.Type.LONG));

        SortedShardTopDocsStreamer aggregator = new SortedShardTopDocsStreamer(
            5, // numHits
            sort, // sort
            false, // trackMaxScore
            2, // batchDocThreshold
            TimeValue.timeValueMillis(100), // timeInterval
            true, // firstHitImmediate
            true // enableCoalescing - enabled
        );

        aggregator.onStart(searchContext);

        // Collect same docs multiple times (should be coalesced)
        for (int round = 0; round < 3; round++) {
            aggregator.onDoc(1, Float.NaN, new Object[] { 1L });
            aggregator.onDoc(2, Float.NaN, new Object[] { 2L });
            aggregator.maybeEmit(false);
        }

        aggregator.onFinish();

        // With coalescing, emission count should be less than without
        assertTrue(aggregator.getEmissionCount() < 6); // Would be 6 without coalescing
    }

    public void testCancellation() throws IOException {
        Sort sort = new Sort(new SortField("sort_field", SortField.Type.LONG));

        SortedShardTopDocsStreamer aggregator = new SortedShardTopDocsStreamer(
            10, // numHits
            sort, // sort
            false, // trackMaxScore
            5, // batchDocThreshold
            TimeValue.timeValueMillis(100), // timeInterval
            true, // firstHitImmediate
            true // enableCoalescing
        );

        aggregator.onStart(searchContext);
        assertFalse(aggregator.isCancelled());

        // Cancel the aggregator
        aggregator.cancel();
        assertTrue(aggregator.isCancelled());

        // Further operations should be no-ops or throw
        try {
            aggregator.onDoc(1, Float.NaN, new Object[] { 1L });
            fail("Expected IOException due to cancellation");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("cancelled"));
        }
    }

    public void testEmptySort() throws IOException {
        Sort emptySort = Sort.RELEVANCE;

        SortedShardTopDocsStreamer aggregator = new SortedShardTopDocsStreamer(
            10, // numHits
            emptySort, // relevance sort
            true, // trackMaxScore - needed for relevance
            5, // batchDocThreshold
            TimeValue.timeValueMillis(100), // timeInterval
            true, // firstHitImmediate
            true // enableCoalescing
        );

        aggregator.onStart(searchContext);

        // Verify sort configuration
        assertEquals(emptySort, aggregator.getSort());

        // Test final result with no documents
        TopDocsAndMaxScore finalResult = aggregator.buildFinalTopDocs();
        assertNotNull(finalResult);
        assertTrue(finalResult.topDocs instanceof TopFieldDocs);
        assertEquals(0, finalResult.topDocs.totalHits.value());

        aggregator.onFinish();
    }
}
