/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class StreamingScoringCollectorTests extends OpenSearchTestCase {

    public void testStreamingScoringCollector() throws Exception {
        // Create a simple index
        Directory directory = new ByteBuffersDirectory();
        IndexWriterConfig config = new IndexWriterConfig();
        IndexWriter writer = new IndexWriter(directory, config);

        // Add some documents
        for (int i = 0; i < 100; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", "doc" + i, Field.Store.YES));
            writer.addDocument(doc);
        }
        writer.close();

        // Search with streaming collector
        DirectoryReader reader = DirectoryReader.open(directory);
        IndexSearcher searcher = new IndexSearcher(reader);

        TopScoreDocCollector delegate = new org.apache.lucene.search.TopScoreDocCollectorManager(10, null, Integer.MAX_VALUE)
            .newCollector();
        StreamingScoringConfig scoringConfig = StreamingScoringConfig.forMode(StreamingSearchMode.CONFIDENCE_BASED);
        TextBoundProvider boundProvider = TextBoundProvider.createDefault();

        List<StreamingScoringCollector.StreamingFrame> emittedFrames = new ArrayList<>();
        AtomicInteger emissionCount = new AtomicInteger(0);

        StreamingScoringCollector collector = new StreamingScoringCollector(delegate, scoringConfig, boundProvider, frame -> {
            emittedFrames.add(frame);
            emissionCount.incrementAndGet();
        });

        // Execute search
        searcher.search(new MatchAllDocsQuery(), collector);

        // Verify results
        assertTrue("Should have emitted some frames", emissionCount.get() > 0);
        assertTrue("Should have collected documents", collector.getEmissionCount() > 0);

        // Verify frame properties
        for (StreamingScoringCollector.StreamingFrame frame : emittedFrames) {
            assertNotNull("Frame should have TopDocs", frame.getTopDocs());
            assertTrue("Sequence number should be positive", frame.getSequenceNumber() > 0);
            assertTrue("Doc count should be positive", frame.getDocCount() > 0);
            assertNotNull("Phase should not be null", frame.getPhase());
            assertTrue("Progress should be between 0 and 1", frame.getProgress() >= 0.0 && frame.getProgress() <= 1.0);
        }

        reader.close();
        directory.close();
    }

    public void testTextBoundProvider() {
        TextBoundProvider provider = TextBoundProvider.createDefault();

        // Test with mock context that has realistic values
        BoundProvider.SearchContext context = new BoundProvider.SearchContext() {
            @Override
            public int getDocCount() {
                return 100;
            }

            @Override
            public org.apache.lucene.search.TopDocs getTopDocs() {
                // Return a mock TopDocs with some score docs
                org.apache.lucene.search.ScoreDoc[] scoreDocs = new org.apache.lucene.search.ScoreDoc[2];
                scoreDocs[0] = new org.apache.lucene.search.ScoreDoc(1, 1.0f);
                scoreDocs[1] = new org.apache.lucene.search.ScoreDoc(2, 0.5f);
                return new org.apache.lucene.search.TopDocs(
                    new org.apache.lucene.search.TotalHits(100, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO),
                    scoreDocs
                );
            }

            @Override
            public float getKthScore() {
                return 0.5f;
            }

            @Override
            public float getMaxPossibleScore() {
                return 1.0f;
            }

            @Override
            public String getModality() {
                return "text";
            }
        };

        double bound = provider.calculateBound(context);
        assertTrue("Bound should be reasonable", bound >= 0.0 && bound <= 1.0);

        boolean isStable = provider.isStable(context);
        assertTrue("Should be stable with 100 docs", isStable);

        double progress = provider.getProgress(context);
        assertTrue("Progress should be reasonable", progress >= 0.0 && progress <= 1.0);

        BoundProvider.SearchPhase phase = provider.getCurrentPhase();
        assertNotNull("Phase should not be null", phase);
    }
}
