/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Tests for {@link LuceneWriterDocValuesFormat} — verifies that the row ID rewriting
 * logic correctly produces sequential values and delegates non-row-ID fields unchanged.
 */
public class LuceneWriterDocValuesFormatTests extends OpenSearchTestCase {

    private static final String ROW_ID_FIELD = LuceneDocumentInput.ROW_ID_FIELD;

    public void testSequentialRowIdDocValuesProducesCorrectValues() {
        int maxDoc = randomIntBetween(5, 100);
        LuceneWriterDocValuesFormat.SequentialRowIdDocValues docValues =
            new LuceneWriterDocValuesFormat.SequentialRowIdDocValues(maxDoc);

        // Test advanceExact
        for (int i = 0; i < maxDoc; i++) {
            assertTrue(docValues.advanceExact(i));
            assertEquals(i, docValues.docID());
            assertEquals(1, docValues.docValueCount());
            assertEquals(i, docValues.nextValue());
        }
    }

    public void testSequentialRowIdDocValuesNextDoc() {
        int maxDoc = randomIntBetween(5, 50);
        LuceneWriterDocValuesFormat.SequentialRowIdDocValues docValues =
            new LuceneWriterDocValuesFormat.SequentialRowIdDocValues(maxDoc);

        for (int i = 0; i < maxDoc; i++) {
            int doc = docValues.nextDoc();
            assertEquals(i, doc);
            assertEquals(i, docValues.nextValue());
        }
        assertEquals(SortedNumericDocValues.NO_MORE_DOCS, docValues.nextDoc());
    }

    public void testSequentialRowIdDocValuesAdvance() {
        int maxDoc = 20;
        LuceneWriterDocValuesFormat.SequentialRowIdDocValues docValues =
            new LuceneWriterDocValuesFormat.SequentialRowIdDocValues(maxDoc);

        // Advance to middle
        int target = 10;
        assertEquals(target, docValues.advance(target));
        assertEquals(target, docValues.nextValue());

        // Advance past end
        assertEquals(SortedNumericDocValues.NO_MORE_DOCS, docValues.advance(maxDoc));
    }

    public void testSequentialRowIdDocValuesCost() {
        int maxDoc = randomIntBetween(1, 1000);
        LuceneWriterDocValuesFormat.SequentialRowIdDocValues docValues =
            new LuceneWriterDocValuesFormat.SequentialRowIdDocValues(maxDoc);
        assertEquals(maxDoc, docValues.cost());
    }

    public void testSequentialRowIdProducerReturnsSortedNumeric() {
        int maxDoc = 10;
        LuceneWriterDocValuesFormat.SequentialRowIdProducer producer =
            new LuceneWriterDocValuesFormat.SequentialRowIdProducer(maxDoc);

        SortedNumericDocValues values = producer.getSortedNumeric(null);
        assertNotNull(values);

        // Other methods return null
        assertNull(producer.getNumeric(null));
        assertNull(producer.getBinary(null));
        assertNull(producer.getSorted(null));
        assertNull(producer.getSortedSet(null));
        assertNull(producer.getSkipper(null));
    }

    public void testGetDelegateReturnsWrappedFormat() {
        DocValuesFormat defaultFormat = Codec.getDefault().docValuesFormat();
        LuceneWriterDocValuesFormat format = new LuceneWriterDocValuesFormat(defaultFormat);
        assertSame(defaultFormat, format.getDelegate());
    }

    public void testRowIdRewriteDuringMerge() throws IOException {
        Path tempDir = createTempDir();
        try (Directory directory = new MMapDirectory(tempDir)) {
            // Create a codec with row ID rewrite enabled
            Codec baseCodec = Codec.getDefault();
            LuceneWriterCodec codec = new LuceneWriterCodec(baseCodec, 1L);

            // Write documents with non-sequential row IDs (simulating a reorder)
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setCodec(codec);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            // Use small buffer to create multiple segments
            iwc.setMaxBufferedDocs(3);
            iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);

            try (IndexWriter writer = new IndexWriter(directory, iwc)) {
                // Write 9 docs with scrambled row IDs across 3 segments
                int[] rowIds = { 5, 8, 2, 7, 0, 3, 6, 1, 4 };
                for (int rowId : rowIds) {
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField(ROW_ID_FIELD, rowId));
                    doc.add(new StringField("id", String.valueOf(rowId), Field.Store.YES));
                    writer.addDocument(doc);
                }
                writer.flush();

                // Now enable row ID rewrite and force merge
                codec.enableRowIdRewrite();
                writer.forceMerge(1);
                writer.commit();
            }

            // Verify that after merge, row IDs are sequential 0..N
            try (IndexReader reader = DirectoryReader.open(directory)) {
                assertEquals(1, reader.leaves().size());
                LeafReader leafReader = reader.leaves().get(0).reader();
                assertEquals(9, leafReader.maxDoc());

                SortedNumericDocValues rowIdValues = leafReader.getSortedNumericDocValues(ROW_ID_FIELD);
                assertNotNull("Row ID doc values should exist after merge", rowIdValues);

                for (int docId = 0; docId < 9; docId++) {
                    assertTrue("Should have value for doc " + docId, rowIdValues.advanceExact(docId));
                    assertEquals(
                        "Row ID should be sequential after rewrite",
                        docId,
                        rowIdValues.nextValue()
                    );
                }
            }
        }
    }

    public void testNonRowIdFieldsAreUnchangedDuringRewrite() throws IOException {
        Path tempDir = createTempDir();
        try (Directory directory = new MMapDirectory(tempDir)) {
            Codec baseCodec = Codec.getDefault();
            LuceneWriterCodec codec = new LuceneWriterCodec(baseCodec, 1L);

            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setCodec(codec);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            iwc.setMaxBufferedDocs(3);
            iwc.setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);

            try (IndexWriter writer = new IndexWriter(directory, iwc)) {
                for (int i = 0; i < 6; i++) {
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField(ROW_ID_FIELD, i * 10)); // non-sequential
                    doc.add(new SortedNumericDocValuesField("score", i * 100)); // user field
                    writer.addDocument(doc);
                }
                writer.flush();

                codec.enableRowIdRewrite();
                writer.forceMerge(1);
                writer.commit();
            }

            try (IndexReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();

                // Row IDs should be rewritten to sequential
                SortedNumericDocValues rowIdValues = leafReader.getSortedNumericDocValues(ROW_ID_FIELD);
                assertNotNull(rowIdValues);
                for (int docId = 0; docId < 6; docId++) {
                    assertTrue(rowIdValues.advanceExact(docId));
                    assertEquals(docId, rowIdValues.nextValue());
                }

                // Score field should retain original values (not rewritten)
                SortedNumericDocValues scoreValues = leafReader.getSortedNumericDocValues("score");
                assertNotNull(scoreValues);
                for (int docId = 0; docId < 6; docId++) {
                    assertTrue(scoreValues.advanceExact(docId));
                    assertEquals(docId * 100, scoreValues.nextValue());
                }
            }
        }
    }

    public void testDocValuesFormatDelegatesWhenRewriteDisabled() throws IOException {
        Path tempDir = createTempDir();
        try (Directory directory = new MMapDirectory(tempDir)) {
            Codec baseCodec = Codec.getDefault();
            LuceneWriterCodec codec = new LuceneWriterCodec(baseCodec, 1L);
            // Do NOT call enableRowIdRewrite()

            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setCodec(codec);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

            try (IndexWriter writer = new IndexWriter(directory, iwc)) {
                for (int i = 0; i < 5; i++) {
                    Document doc = new Document();
                    doc.add(new SortedNumericDocValuesField(ROW_ID_FIELD, i * 10));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
                writer.commit();
            }

            // Without rewrite enabled, row IDs should retain their original values
            try (IndexReader reader = DirectoryReader.open(directory)) {
                LeafReader leafReader = reader.leaves().get(0).reader();
                SortedNumericDocValues rowIdValues = leafReader.getSortedNumericDocValues(ROW_ID_FIELD);
                assertNotNull(rowIdValues);
                for (int docId = 0; docId < 5; docId++) {
                    assertTrue(rowIdValues.advanceExact(docId));
                    assertEquals(docId * 10, rowIdValues.nextValue());
                }
            }
        }
    }
}
