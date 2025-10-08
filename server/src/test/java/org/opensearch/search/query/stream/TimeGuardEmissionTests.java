/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query.stream;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.TopDocs;
import org.opensearch.action.support.StreamSearchChannelListener;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Time-guard emission tests for streaming aggregators.
 */
public class TimeGuardEmissionTests extends OpenSearchTestCase {

    private SearchContext searchContext;
    private StreamSearchChannelListener channelListener;
    private AtomicInteger emissionCounter;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        searchContext = mock(SearchContext.class);
        channelListener = mock(StreamSearchChannelListener.class);
        emissionCounter = new AtomicInteger(0);

        when(searchContext.getStreamChannelListener()).thenReturn(channelListener);
        when(searchContext.getStreamingBatchSize()).thenReturn(1000); // High to avoid batch emissions
        when(searchContext.getStreamingTimeInterval()).thenReturn(TimeValue.timeValueMillis(50));
        when(searchContext.getStreamingFirstHitImmediate()).thenReturn(false); // Disable for this test
        when(searchContext.getStreamingEnableCoalescing()).thenReturn(false);

        // Count emissions
        doAnswer(invocation -> {
            emissionCounter.incrementAndGet();
            return null;
        }).when(channelListener).onStreamResponse(any(QuerySearchResult.class), anyBoolean());
    }

    @After
    public void tearDown() throws Exception {
        // Clean up the fallback scheduler to prevent thread leaks
        AbstractShardTopDocsStreamer.shutdownFallbackScheduler();
        super.tearDown();
    }

    public void testSchedulerEmission() throws IOException, InterruptedException {
        // Create a test aggregator with a short time interval
        TestStreamingAggregator aggregator = new TestStreamingAggregator(
            1000, // high batch threshold to avoid batch emissions
            TimeValue.timeValueMillis(100), // 100ms time interval
            false, // firstHitImmediate disabled
            false  // coalescing disabled
        );

        aggregator.onStart(searchContext);

        assertEquals("Should have no emissions initially", 0, emissionCounter.get());

        // Initially no documents - scheduler should not emit empty snapshots
        Thread.sleep(120); // Sleep longer than the interval
        assertEquals("Should not emit empty snapshots", 0, emissionCounter.get());

        // Collect a document - now scheduler can emit
        aggregator.onDoc(1, 1.0f, null);
        assertEquals("Should not emit immediately due to firstHitImmediate=false", 0, emissionCounter.get());

        // Wait for scheduler to emit
        Thread.sleep(120); // Wait for next scheduled emission
        assertTrue("Should have emitted due to scheduler", emissionCounter.get() > 0);

        aggregator.onFinish();
    }

    public void testTimeWithBatch() throws IOException, InterruptedException {
        TestStreamingAggregator aggregator = new TestStreamingAggregator(
            2, // low batch threshold to trigger batch emission
            TimeValue.timeValueMillis(50), // 50ms time interval
            false, // firstHitImmediate disabled
            false  // coalescing disabled
        );

        aggregator.onStart(searchContext);

        // Collect documents to trigger a batch emission first
        aggregator.onDoc(1, 1.0f, null);
        aggregator.onDoc(2, 1.0f, null); // This should trigger batch emission

        int emissionsAfterBatch = emissionCounter.get();
        assertTrue("Should have emitted due to batch threshold", emissionsAfterBatch > 0);

        // Wait for time interval to pass (time is measured from last emission)
        Thread.sleep(60);

        // Collect another document - this SHOULD trigger time-guard emission
        // because time interval has passed since last emission
        aggregator.onDoc(3, 1.0f, null);

        // Should have additional time-guard emission
        assertTrue("Should have time-guard emission after time interval", emissionCounter.get() > emissionsAfterBatch);

        aggregator.onFinish();
    }

    public void testZeroInterval() throws IOException {
        TestStreamingAggregator aggregator = new TestStreamingAggregator(
            1000, // high batch threshold
            TimeValue.timeValueMillis(0), // zero interval should disable time guard
            false, // firstHitImmediate disabled
            false  // coalescing disabled
        );

        aggregator.onStart(searchContext);

        // Collect documents - should not emit due to time guard with zero interval
        aggregator.onDoc(1, 1.0f, null);
        aggregator.onDoc(2, 1.0f, null);

        assertEquals("Should not emit with zero time interval", 0, emissionCounter.get());

        aggregator.onFinish();
    }

    public void testNoEmptyEmissions() throws IOException, InterruptedException {
        TestStreamingAggregator aggregator = new TestStreamingAggregator(
            1000, // high batch threshold
            TimeValue.timeValueMillis(50), // 50ms time interval
            false, // firstHitImmediate disabled
            false  // coalescing disabled
        );

        aggregator.onStart(searchContext);

        assertEquals("Should have no emissions initially", 0, emissionCounter.get());

        // Wait well beyond the time interval
        Thread.sleep(100); // Sleep longer than the 50ms interval

        // Should still have no emissions because onDoc was never called
        assertEquals("Should not emit without onDoc call, even after time interval", 0, emissionCounter.get());

        aggregator.onFinish();
    }

    public void testPeriodicEmissions() throws IOException, InterruptedException {
        TestStreamingAggregator aggregator = new TestStreamingAggregator(
            1000, // high batch threshold to avoid batch emissions
            TimeValue.timeValueMillis(80), // 80ms time interval
            false, // firstHitImmediate disabled
            false  // coalescing disabled
        );

        aggregator.onStart(searchContext);

        assertEquals("Should have no emissions initially", 0, emissionCounter.get());

        // Collect documents to have something to emit
        aggregator.onDoc(1, 1.0f, null);
        aggregator.onDoc(2, 1.0f, null);
        assertEquals("Should not emit immediately", 0, emissionCounter.get());

        // Wait for first scheduled emission
        Thread.sleep(100); // Wait past first interval
        int firstEmission = emissionCounter.get();
        assertTrue("Should have first time-based emission", firstEmission >= 1);

        // Add another document and wait for next emission
        aggregator.onDoc(3, 1.0f, null);
        Thread.sleep(100); // Wait past second interval
        int secondEmission = emissionCounter.get();
        assertTrue("Should have second time-based emission", secondEmission > firstEmission);

        // Add another document and wait for third emission
        aggregator.onDoc(4, 1.0f, null);
        Thread.sleep(100); // Wait past third interval
        int thirdEmission = emissionCounter.get();
        assertTrue("Should have third time-based emission", thirdEmission > secondEmission);

        aggregator.onFinish();
    }

    public void testCoalescing() throws IOException, InterruptedException {
        TestStreamingAggregator aggregator = new TestStreamingAggregator(
            1000, // high batch threshold to avoid batch emissions
            TimeValue.timeValueMillis(60), // 60ms time interval
            false, // firstHitImmediate disabled
            true   // coalescing enabled
        );

        aggregator.onStart(searchContext);

        // Collect documents
        aggregator.onDoc(1, 1.0f, null);
        aggregator.onDoc(2, 1.0f, null);

        // Wait for first emission
        Thread.sleep(80);
        int firstEmission = emissionCounter.get();
        assertTrue("Should have first emission", firstEmission >= 1);

        // Don't add more documents - scheduler should coalesce (skip emission)
        Thread.sleep(80); // Wait for next interval
        int secondCheck = emissionCounter.get();
        assertEquals("Should coalesce unchanged snapshot", firstEmission, secondCheck);

        // Add another document - should emit again
        aggregator.onDoc(3, 1.0f, null);
        Thread.sleep(80);
        int thirdCheck = emissionCounter.get();
        assertTrue("Should emit after snapshot changes", thirdCheck > secondCheck);

        aggregator.onFinish();
    }

    // Test aggregator
    private static class TestStreamingAggregator extends AbstractShardTopDocsStreamer {
        private int docCount = 0;

        TestStreamingAggregator(int batchDocThreshold, TimeValue timeInterval, boolean firstHitImmediate, boolean enableCoalescing) {
            super(batchDocThreshold, timeInterval, firstHitImmediate, enableCoalescing);
        }

        @Override
        protected void processDocument(int globalDocId, float score, Object[] sortValues) throws IOException {
            docCount++;
        }

        @Override
        protected TopDocs buildCurrentTopDocs() throws IOException {
            // Create a simple TopDocs with current doc count
            org.apache.lucene.search.ScoreDoc[] scoreDocs = new org.apache.lucene.search.ScoreDoc[Math.min(docCount, 5)];
            for (int i = 0; i < scoreDocs.length; i++) {
                scoreDocs[i] = new org.apache.lucene.search.ScoreDoc(i, 1.0f);
            }
            return new TopDocs(
                new org.apache.lucene.search.TotalHits(docCount, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
                scoreDocs
            );
        }

        @Override
        public LeafCollector newLeafCollector(LeafReaderContext context, Scorable scorable) throws IOException {
            return new LeafCollector() {
                @Override
                public void setScorer(Scorable scorer) throws IOException {}

                @Override
                public void collect(int doc) throws IOException {
                    onDoc(context.docBase + doc, 1.0f, null);
                }
            };
        }

        @Override
        public TopDocsAndMaxScore buildFinalTopDocs() throws IOException {
            TopDocs topDocs = buildCurrentTopDocs();
            return new TopDocsAndMaxScore(topDocs, Float.NaN);
        }
    }
}
