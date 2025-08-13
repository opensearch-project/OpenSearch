/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Unit tests for FieldLevelStatsCache
 */
public class FieldLevelStatsCacheTests extends OpenSearchTestCase {

    /**
     * Test basic cache put and get operations
     */
    public void testBasicCacheOperations() throws IOException {
        FieldLevelStatsCache cache = new FieldLevelStatsCache();

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER))) {
            // Create a segment
            Document doc = new Document();
            doc.add(new TextField("field", "content", Field.Store.YES));
            writer.addDocument(doc);
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();

                // Initially cache should return null
                assertNull("Cache should be empty initially", cache.get(segmentReader));

                // Put stats in cache
                Map<String, Map<String, Long>> stats = Map.of("field", Map.of("tim", 100L, "tip", 10L));
                cache.put(segmentReader, stats);

                // Get should return the stats
                Map<String, Map<String, Long>> cachedStats = cache.get(segmentReader);
                assertNotNull("Should get cached stats", cachedStats);
                assertEquals("Cached stats should match", stats, cachedStats);
            }
        }
    }

    /**
     * Test cache invalidation
     */
    public void testCacheInvalidation() throws IOException {
        FieldLevelStatsCache cache = new FieldLevelStatsCache();

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER))) {

            Document doc = new Document();
            doc.add(new TextField("field", "content", Field.Store.YES));
            writer.addDocument(doc);
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();

                // Put stats in cache
                Map<String, Map<String, Long>> stats = Map.of("field", Map.of("tim", 100L, "tip", 10L));
                cache.put(segmentReader, stats);

                // Verify stats are cached
                assertNotNull("Should have cached stats", cache.get(segmentReader));

                // Invalidate the cache entry
                cache.invalidate(segmentReader);

                // Should return null after invalidation
                assertNull("Should return null after invalidation", cache.get(segmentReader));
            }
        }
    }

    /**
     * Test cache clear operation
     */
    public void testCacheClear() throws IOException {
        FieldLevelStatsCache cache = new FieldLevelStatsCache();

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER))) {

            // Create multiple segments
            for (int i = 0; i < 3; i++) {
                Document doc = new Document();
                doc.add(new TextField("field" + i, "content" + i, Field.Store.YES));
                writer.addDocument(doc);
                writer.commit();
            }

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                // Cache stats for all segments
                for (int i = 0; i < reader.leaves().size(); i++) {
                    SegmentReader segmentReader = (SegmentReader) reader.leaves().get(i).reader();
                    Map<String, Map<String, Long>> stats = Map.of("field" + i, Map.of("tim", 100L * (i + 1), "tip", 10L * (i + 1)));
                    cache.put(segmentReader, stats);
                }

                // Verify all are cached
                for (int i = 0; i < reader.leaves().size(); i++) {
                    SegmentReader segmentReader = (SegmentReader) reader.leaves().get(i).reader();
                    assertNotNull("Should have cached stats for segment " + i, cache.get(segmentReader));
                }

                // Clear cache
                cache.clear();

                // Verify all are cleared
                for (int i = 0; i < reader.leaves().size(); i++) {
                    SegmentReader segmentReader = (SegmentReader) reader.leaves().get(i).reader();
                    assertNull("Should have no cached stats after clear", cache.get(segmentReader));
                }
            }
        }
    }

    /**
     * Test cache with multiple segments from same index
     */
    public void testMultipleSegments() throws IOException {
        FieldLevelStatsCache cache = new FieldLevelStatsCache();

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER))) {

            // Create multiple segments
            for (int i = 0; i < 5; i++) {
                Document doc = new Document();
                doc.add(new TextField("field", "content " + i, Field.Store.YES));
                writer.addDocument(doc);
                if (i % 2 == 0) {
                    writer.commit(); // Create new segment every 2 docs
                }
            }
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                assertTrue("Should have multiple segments", reader.leaves().size() > 1);

                // Cache different stats for each segment
                for (int i = 0; i < reader.leaves().size(); i++) {
                    SegmentReader segmentReader = (SegmentReader) reader.leaves().get(i).reader();
                    Map<String, Map<String, Long>> stats = Map.of("field", Map.of("tim", 100L * (i + 1), "tip", 10L * (i + 1)));
                    cache.put(segmentReader, stats);
                }

                // Verify each segment has its own cached stats
                for (int i = 0; i < reader.leaves().size(); i++) {
                    SegmentReader segmentReader = (SegmentReader) reader.leaves().get(i).reader();
                    Map<String, Map<String, Long>> cachedStats = cache.get(segmentReader);
                    assertNotNull("Should have cached stats for segment " + i, cachedStats);

                    // Verify the stats are correct for this segment
                    long expectedTim = 100L * (i + 1);
                    assertEquals("Stats should match for segment " + i, expectedTim, (long) cachedStats.get("field").get("tim"));
                }
            }
        }
    }

    /**
     * Test concurrent cache access
     */
    public void testConcurrentAccess() throws Exception {
        final FieldLevelStatsCache cache = new FieldLevelStatsCache();
        final int numThreads = 10;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch completeLatch = new CountDownLatch(numThreads);
        final AtomicInteger errors = new AtomicInteger(0);

        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER))) {

            Document doc = new Document();
            doc.add(new TextField("field", "content", Field.Store.YES));
            writer.addDocument(doc);
            writer.commit();

            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                final SegmentReader segmentReader = (SegmentReader) reader.leaves().get(0).reader();
                final Map<String, Map<String, Long>> stats = Map.of("field", Map.of("tim", 100L, "tip", 10L));

                // Create threads that will access cache concurrently
                for (int i = 0; i < numThreads; i++) {
                    final int threadId = i;
                    new Thread(() -> {
                        try {
                            startLatch.await();

                            // Each thread performs multiple operations
                            for (int j = 0; j < 100; j++) {
                                if (threadId % 3 == 0) {
                                    // Some threads just read
                                    cache.get(segmentReader);
                                } else if (threadId % 3 == 1) {
                                    // Some threads write
                                    cache.put(segmentReader, stats);
                                } else {
                                    // Some threads invalidate
                                    if (j % 10 == 0) {
                                        cache.invalidate(segmentReader);
                                    } else {
                                        cache.get(segmentReader);
                                    }
                                }
                            }
                        } catch (Exception e) {
                            errors.incrementAndGet();
                            logger.error("Error in concurrent test", e);
                        } finally {
                            completeLatch.countDown();
                        }
                    }).start();
                }

                // Start all threads at once
                startLatch.countDown();

                // Wait for completion
                assertTrue("Threads should complete", completeLatch.await(10, TimeUnit.SECONDS));
                assertEquals("Should have no errors", 0, errors.get());

                // Cache should still be functional
                cache.put(segmentReader, stats);
                assertEquals("Cache should still work", stats, cache.get(segmentReader));
            }
        }
    }
}
