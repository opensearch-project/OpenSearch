/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.SegmentInfoFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.exec.DocumentMetadataResolver.DocumentMetadata;
import org.opensearch.index.engine.exec.IndexReaderProvider;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LuceneDocumentResolverTests extends OpenSearchTestCase {

    private final LuceneDocumentResolver resolver = new LuceneDocumentResolver();

    // --- resolveMetadata ---

    /**
     * When the index has no Lucene secondary reader, {@code directoryReader(reader)} fails fast:
     * resolution cannot proceed without a Lucene reader, so it throws rather than returning a bogus result.
     */
    public void testResolveMetadata_emptyReader_returnsNull() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                w.commit();
            }
            try (DirectoryReader dr = DirectoryReader.open(dir)) {
                assertNull(resolver.resolveMetadata(readerReturning(dr), "doc1"));
            }
        }
    }

    public void testResolveMetadata_idNotPresent_returnsNull() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, iwc(codecWithGeneration("1")))) {
                addDoc(w, "other", 1L, 1L, 1L, 0L, true);
            }
            try (DirectoryReader dr = DirectoryReader.open(dir)) {
                assertNull(resolver.resolveMetadata(readerReturning(dr), "missing"));
            }
        }
    }

    public void testResolveMetadata_found_returnsMetadata() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, iwc(codecWithGeneration("5")))) {
                addDoc(w, "doc1", 7L, 42L, 3L, 9L, true);
            }
            try (DirectoryReader dr = DirectoryReader.open(dir)) {
                DocumentMetadata md = resolver.resolveMetadata(readerReturning(dr), "doc1");
                assertNotNull(md);
                assertEquals("doc1", md.id());
                assertEquals(9L, md.rowId());
                assertEquals(5L, md.writerGeneration());
            }
        }
    }

    public void testResolveMetadata_missingWriterGeneration_throwsISE() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            // default codec → no writer_generation segment attribute
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig())) {
                addDoc(w, "doc1", 1L, 1L, 1L, 0L, true);
            }
            try (DirectoryReader dr = DirectoryReader.open(dir)) {
                IllegalStateException e = expectThrows(
                    IllegalStateException.class,
                    () -> resolver.resolveMetadata(readerReturning(dr), "doc1")
                );
                assertTrue(e.getMessage().contains("writer_generation"));
            }
        }
    }

    public void testResolveMetadata_missingRowIdDocValues_throwsISE() throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, iwc(codecWithGeneration("1")))) {
                addDoc(w, "doc1", 1L, 1L, 1L, 0L, false); // no __row_id__ doc values
            }
            try (DirectoryReader dr = DirectoryReader.open(dir)) {
                IllegalStateException e = expectThrows(
                    IllegalStateException.class,
                    () -> resolver.resolveMetadata(readerReturning(dr), "doc1")
                );
                assertTrue(e.getMessage().contains(DocumentInput.ROW_ID_FIELD));
            }
        }
    }

    // --- helpers ---

    private static IndexReaderProvider.Reader readerReturning(DirectoryReader dr) {
        IndexReaderProvider.Reader reader = mock(IndexReaderProvider.Reader.class);
        when(reader.getReader(any(DataFormat.class), eq(LuceneReader.class))).thenReturn(new LuceneReader(dr, java.util.Map.of()));
        return reader;
    }

    private static IndexWriterConfig iwc(Codec codec) {
        IndexWriterConfig c = new IndexWriterConfig();
        c.setCodec(codec);
        return c;
    }

    private static void addDoc(IndexWriter w, String id, long version, long seqNo, long primaryTerm, long rowId, boolean withRowId)
        throws IOException {
        Document doc = new Document();
        doc.add(new StringField(IdFieldMapper.NAME, Uid.encodeId(id), Field.Store.NO)); // for _id TermQuery
        doc.add(new StoredField(IdFieldMapper.NAME, Uid.encodeId(id)));                  // for decodeId (restore path)
        doc.add(new NumericDocValuesField(VersionFieldMapper.NAME, version));
        doc.add(new LongPoint(SeqNoFieldMapper.NAME, seqNo));                            // for LongPoint range scan
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.NAME, seqNo));
        doc.add(new NumericDocValuesField(SeqNoFieldMapper.PRIMARY_TERM_NAME, primaryTerm));
        if (withRowId) {
            doc.add(new SortedNumericDocValuesField(DocumentInput.ROW_ID_FIELD, rowId));
        }
        w.addDocument(doc);
        w.commit();
    }

    /** Codec that stamps a {@code writer_generation} segment attribute, mirroring the production writer. */
    private static Codec codecWithGeneration(String gen) {
        Codec base = Codec.getDefault();
        return new FilterCodec(base.getName(), base) {
            @Override
            public SegmentInfoFormat segmentInfoFormat() {
                SegmentInfoFormat delegate = base.segmentInfoFormat();
                return new SegmentInfoFormat() {
                    @Override
                    public SegmentInfo read(Directory d, String name, byte[] id, IOContext ctx) throws IOException {
                        return delegate.read(d, name, id, ctx);
                    }

                    @Override
                    public void write(Directory d, SegmentInfo info, IOContext ctx) throws IOException {
                        info.putAttribute("writer_generation", gen);
                        delegate.write(d, info, ctx);
                    }
                };
            }
        };
    }
}
