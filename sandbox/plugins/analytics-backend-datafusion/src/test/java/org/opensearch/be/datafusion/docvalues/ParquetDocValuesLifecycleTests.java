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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
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
     * Two sequential requests over the same segment core reuse the one registry-cached producer but
     * receive independent cursors, and closing one request does not disturb the other's cursor.
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
     * The registry caches one producer per core cache key and closes it from the core's
     * closed-listener: closing the segment reader that owns the core closes the producer and drops it.
     */
    public void testSegmentCoreCloseClosesTheProducer() throws Exception {
        ParquetDocValuesProducer producer = new ParquetDocValuesProducer(
            createTempDir().resolve("core.parquet"),
            ParquetColumnReader.LOCAL_STORE,
            Settings.EMPTY,
            1,
            null
        );

        Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());
        Document doc = new Document();
        doc.add(new StringField("id", "1", Field.Store.NO));
        writer.addDocument(doc);
        writer.commit();

        DirectoryReader reader = DirectoryReader.open(dir);
        try {
            IndexReader.CacheHelper coreHelper = reader.leaves().get(0).reader().getCoreCacheHelper();
            assertNotNull("segment leaf must expose a core cache helper", coreHelper);

            int before = ParquetDocValuesProducerRegistry.size();
            ParquetDocValuesProducer got = ParquetDocValuesProducerRegistry.getOrCreate(coreHelper, () -> producer);
            assertSame("first getOrCreate must return the created producer", producer, got);
            assertEquals(before + 1, ParquetDocValuesProducerRegistry.size());

            // A second request over the same core reuses the cached producer; the factory is not run.
            ParquetDocValuesProducer reused = ParquetDocValuesProducerRegistry.getOrCreate(coreHelper, () -> {
                throw new AssertionError("factory must not run on a cache hit");
            });
            assertSame(producer, reused);
            assertFalse(producer.isClosed());

            reader.close(); // drops the core -> fires the closed-listener
            assertTrue("core close must close the producer", producer.isClosed());
            assertEquals("closed producer must be dropped from the registry", before, ParquetDocValuesProducerRegistry.size());
        } finally {
            if (reader.getRefCount() > 0) {
                reader.close();
            }
            writer.close();
            dir.close();
        }
    }

    /** A synthetic SORTED_NUMERIC field info, matching what the leaf wrapper synthesizes. */
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
