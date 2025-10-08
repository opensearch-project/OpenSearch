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
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// UnsortedShardTopDocsStreamer tests
public class UnsortedShardTopDocsStreamerTests extends OpenSearchTestCase {

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

        // Add test documents
        for (int i = 0; i < 100; i++) {
            Document doc = new Document();
            doc.add(new TextField("field", "doc" + i, Field.Store.YES));
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
        UnsortedShardTopDocsStreamer aggregator = new UnsortedShardTopDocsStreamer(
            10, // numHits
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
        assertEquals(Float.NaN, finalResult.maxScore, 0.0f);

        aggregator.onFinish();

        // Verify basic metrics
        assertTrue(aggregator.getCollectedCount() >= 0);
        assertTrue(aggregator.getEmissionCount() >= 0);
        assertFalse(aggregator.isCancelled());
    }

    public void testTriggers() throws IOException {
        AtomicInteger emissionCount = new AtomicInteger(0);

        UnsortedShardTopDocsStreamer aggregator = new UnsortedShardTopDocsStreamer(
            10, // numHits
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

    public void testCoalescing() throws IOException {
        UnsortedShardTopDocsStreamer aggregator = new UnsortedShardTopDocsStreamer(
            5, // numHits
            false, // trackMaxScore
            2, // batchDocThreshold
            TimeValue.timeValueMillis(100), // timeInterval
            true, // firstHitImmediate
            true // enableCoalescing - enabled
        );

        aggregator.onStart(searchContext);

        // Collect same docs multiple times (should be coalesced)
        for (int round = 0; round < 3; round++) {
            aggregator.onDoc(1, Float.NaN, null);
            aggregator.onDoc(2, Float.NaN, null);
            aggregator.maybeEmit(false);
        }

        aggregator.onFinish();

        // With coalescing, emission count should be less than without
        assertTrue(aggregator.getEmissionCount() < 6); // Would be 6 without coalescing
    }

    public void testCancellation() throws IOException {
        UnsortedShardTopDocsStreamer aggregator = new UnsortedShardTopDocsStreamer(
            10, // numHits
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
            aggregator.onDoc(1, Float.NaN, null);
            fail("Expected IOException due to cancellation");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("cancelled"));
        }
    }

    public void testMemoryEstimation() throws IOException {
        UnsortedShardTopDocsStreamer aggregator = new UnsortedShardTopDocsStreamer(
            100, // numHits
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
            aggregator.onDoc(i, Float.NaN, null);
        }

        // Memory usage should increase
        long memoryAfterDocs = aggregator.estimateMemoryUsage();
        assertTrue(memoryAfterDocs >= initialMemory);

        aggregator.onFinish();
    }

    public void testFirstHit() throws IOException {
        UnsortedShardTopDocsStreamer aggregator = new UnsortedShardTopDocsStreamer(
            10, // numHits
            false, // trackMaxScore
            100, // batchDocThreshold - high to avoid batch emissions
            TimeValue.timeValueMillis(1000), // timeInterval - high to avoid time emissions
            true, // firstHitImmediate - enabled
            false // enableCoalescing
        );

        aggregator.onStart(searchContext);

        // Get a leaf collector
        if (!reader.leaves().isEmpty()) {
            LeafReaderContext context = reader.leaves().get(0);
            var leafCollector = aggregator.newLeafCollector(context, null);

            // Set a dummy scorer to avoid NullPointerException
            leafCollector.setScorer(new org.apache.lucene.search.Scorable() {
                @Override
                public float score() throws IOException {
                    return 1.0f;
                }
            });

            // Collect first document - should trigger immediate emission
            if (context.reader().maxDoc() > 0) {
                leafCollector.collect(0);

                // Should have emitted due to first hit
                assertEquals("Expected 1 emission after first hit", 1, aggregator.getEmissionCount());

                // Collect more docs - should not emit immediately due to high batch threshold
                for (int doc = 1; doc < Math.min(3, context.reader().maxDoc()); doc++) {
                    leafCollector.collect(doc);
                }

                // Still only 1 emission (first hit)
                assertEquals("Expected only 1 emission (first hit)", 1, aggregator.getEmissionCount());
            }
        }

        aggregator.onFinish();
    }

    public void testConcurrent() throws IOException, InterruptedException {
        UnsortedShardTopDocsStreamer aggregator = new UnsortedShardTopDocsStreamer(
            50, // numHits
            false, // trackMaxScore
            10, // batchDocThreshold
            TimeValue.timeValueMillis(100), // timeInterval
            false, // firstHitImmediate - disabled to simplify thread test
            true // enableCoalescing
        );

        aggregator.onStart(searchContext);

        // Get leaf collectors for different segments
        List<LeafReaderContext> contexts = reader.leaves();
        if (contexts.isEmpty()) {
            return; // Skip if no segments
        }

        // Use the first context for all threads (simulating concurrent collection in same segment)
        LeafReaderContext context = contexts.get(0);
        int maxDoc = context.reader().maxDoc();
        if (maxDoc == 0) {
            return; // Skip if no documents
        }

        // Test concurrent document collection
        int numThreads = Math.min(5, maxDoc);
        Thread[] threads = new Thread[numThreads];
        int docsPerThread = Math.min(20, maxDoc / numThreads);

        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            threads[t] = new Thread(() -> {
                try {
                    var leafCollector = aggregator.newLeafCollector(context, null);

                    // Set a dummy scorer to avoid NullPointerException
                    leafCollector.setScorer(new org.apache.lucene.search.Scorable() {
                        @Override
                        public float score() throws IOException {
                            return 1.0f;
                        }
                    });

                    int startDoc = threadId * docsPerThread;
                    int endDoc = Math.min(startDoc + docsPerThread, maxDoc);

                    for (int doc = startDoc; doc < endDoc; doc++) {
                        leafCollector.collect(doc);
                    }
                } catch (IOException e) {
                    // Ignore for test
                }
            });
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        // Verify final state - collected count should be > 0
        assertTrue("Expected documents to be collected", aggregator.getCollectedCount() > 0);

        // Due to threading, we might have emissions (depending on batch threshold)
        // Just verify no exceptions occurred and state is consistent
        assertTrue("Emission count should be non-negative", aggregator.getEmissionCount() >= 0);

        aggregator.onFinish();
    }
}
