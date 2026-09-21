/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.docvalues;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.opensearch.be.datafusion.docvalues.bridge.DataFusionBackedTestCase;
import org.opensearch.be.datafusion.docvalues.bridge.ParquetColumnReader;
import org.opensearch.common.settings.Settings;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Producer-lifecycle coverage: the segment-lifetime producer is shared across requests and closed by
 * the segment core, while each request owns and closes only the cursors it opened.
 */
public class ParquetDocValuesLifecycleTests extends DataFusionBackedTestCase {

    private static final String COLUMN = "value";

    /**
     * Every accessor call hands out a fresh cursor (dedicated per consumer), and closing the request's
     * registry closes exactly those cursors and leaves the shared producer open. Close is idempotent.
     */
    public void testRequestEndClosesOnlyItsCursorsNotTheProducer() throws Exception {
        int rows = 200;
        Path file = createTempDir().resolve("lifecycle.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, rows, -1);

        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(file, ParquetColumnReader.LOCAL_STORE, Settings.EMPTY, rows, null);
        FieldInfo fi = sortedNumericField(COLUMN);

        CursorRegistry request = new CursorRegistry();
        SortedNumericDocValues first = producer.getSortedNumeric(fi, request);
        producer.getSortedNumeric(fi, request);

        List<ParquetColumnReader> opened = request.opened();
        assertEquals("each accessor call opens its own cursor", 2, opened.size());
        assertNotSame("cursors must be dedicated per consumer", opened.get(0), opened.get(1));

        // The cursor reads real values off the fixture before the request ends.
        NumericDocValues single = DocValues.unwrapSingleton(first);
        assertTrue(single.advanceExact(10));
        assertEquals(LongColumnFixture.valueAt(10), single.longValue());

        for (ParquetColumnReader cursor : opened) {
            assertFalse("cursor must be open mid-request", cursor.isClosed());
        }

        request.close();

        for (ParquetColumnReader cursor : opened) {
            assertTrue("every request cursor must be closed at request end", cursor.isClosed());
        }
        assertFalse("producer must outlive the request", producer.isClosed());

        request.close(); // idempotent
    }

    /**
     * Two sequential requests over the same segment core reuse the one cached producer but receive
     * independent cursors, and closing one request does not disturb the other's cursor.
     */
    public void testSequentialRequestsReuseProducerButGetDistinctCursors() throws Exception {
        int rows = 200;
        Path file = createTempDir().resolve("reuse.parquet");
        LongColumnFixture.write(file, allocator, COLUMN, rows, -1);

        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(file, ParquetColumnReader.LOCAL_STORE, Settings.EMPTY, rows, null);
        FieldInfo fi = sortedNumericField(COLUMN);

        CursorRegistry firstRequest = new CursorRegistry();
        producer.getSortedNumeric(fi, firstRequest);
        ParquetColumnReader firstCursor = firstRequest.opened().get(0);

        CursorRegistry secondRequest = new CursorRegistry();
        producer.getSortedNumeric(fi, secondRequest);
        ParquetColumnReader secondCursor = secondRequest.opened().get(0);

        assertNotSame("sequential requests must not share a cursor", firstCursor, secondCursor);

        firstRequest.close();
        assertTrue("first request's cursor must close with it", firstCursor.isClosed());
        assertFalse("second request's cursor must be untouched", secondCursor.isClosed());

        secondRequest.close();
        assertTrue(secondCursor.isClosed());
    }

    /**
     * A second wrap over the same segment core reuses the same {@link ParquetSegmentResources} instance and
     * caches no additional entry; the resources built on the first wrap are not rebuilt.
     */
    public void testSecondWrapOverSameCoreReusesResources() throws Exception {
        ParquetDocValuesProducer producer = newFixtureProducer("identity.parquet");
        ParquetSegmentResources resources = new ParquetSegmentResources(producer, Map.of(), new FieldInfos(new FieldInfo[0]));

        Directory dir = newDirectory();
        IndexWriter writer = singleDocWriter(dir);
        ParquetSegmentResourceCache cache = new ParquetSegmentResourceCache(null);
        DirectoryReader reader = DirectoryReader.open(dir);
        try {
            LeafReader leaf = reader.leaves().get(0).reader();

            int before = cache.size();
            ParquetSegmentResources first = cache.cacheForTesting(leaf, resources);
            assertSame("first wrap installs the resources", resources, first);
            assertEquals(before + 1, cache.size());

            // The second wrap passes distinct resources, but the cache returns the first instance and
            // records nothing new: the per-core resources are resolved once.
            ParquetSegmentResources second = cache.cacheForTesting(
                leaf,
                new ParquetSegmentResources(producer, Map.of(), new FieldInfos(new FieldInfo[0]))
            );
            assertSame("second wrap over the same core reuses the same resources instance", resources, second);
            assertEquals("second wrap adds no cache entry", before + 1, cache.size());
        } finally {
            reader.close();
            writer.close();
            dir.close();
        }
    }

    /**
     * The cache holds one resources instance per core cache key and closes its producer from the core's
     * closed-listener: closing the segment reader that owns the core closes the producer and drops the
     * entry.
     */
    public void testSegmentCoreCloseClosesTheProducer() throws Exception {
        ParquetDocValuesProducer producer = newFixtureProducer("core.parquet");
        ParquetSegmentResources resources = new ParquetSegmentResources(producer, Map.of(), new FieldInfos(new FieldInfo[0]));

        Directory dir = newDirectory();
        IndexWriter writer = singleDocWriter(dir);
        ParquetSegmentResourceCache cache = new ParquetSegmentResourceCache(null);
        DirectoryReader reader = DirectoryReader.open(dir);
        try {
            LeafReader leaf = reader.leaves().get(0).reader();
            assertNotNull("segment leaf must expose a core cache helper", leaf.getCoreCacheHelper());

            int before = cache.size();
            cache.cacheForTesting(leaf, resources);
            assertEquals(before + 1, cache.size());
            assertFalse(producer.isClosed());

            reader.close(); // drops the core -> fires the closed-listener
            assertTrue("core close must close the producer", producer.isClosed());
            assertEquals("closed resources must be dropped from the cache", before, cache.size());
        } finally {
            if (reader.getRefCount() > 0) {
                reader.close();
            }
            writer.close();
            dir.close();
        }
    }

    private ParquetDocValuesProducer newFixtureProducer(String name) {
        return new ParquetDocValuesProducer(createTempDir().resolve(name), ParquetColumnReader.LOCAL_STORE, Settings.EMPTY, 1, null);
    }

    private static IndexWriter singleDocWriter(Directory dir) throws Exception {
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        Document doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();
        return writer;
    }

    /** A synthetic SORTED_NUMERIC field info, matching what the resources builder synthesizes. */
    private static FieldInfo sortedNumericField(String name) {
        return new FieldInfo(
            name,
            0,
            false,
            true,
            false,
            IndexOptions.NONE,
            DocValuesType.SORTED_NUMERIC,
            DocValuesSkipIndexType.NONE,
            -1,
            new HashMap<>(),
            0,
            0,
            0,
            0,
            VectorEncoding.FLOAT32,
            VectorSimilarityFunction.EUCLIDEAN,
            false,
            false
        );
    }
}
