/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.util.BytesRef;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.index.engine.dataformat.DeleteInput;
import org.opensearch.index.engine.dataformat.DeleteResult;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.index.engine.dataformat.DeleterImpl;

import java.io.IOException;
import java.nio.file.Path;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DeleterImpl}.
 */
public class DeleterImplTests extends OpenSearchTestCase {

    private LuceneDataFormat dataFormat;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataFormat = new LuceneDataFormat();
    }

    private LuceneWriter createWriter(Path baseDir, long generation) throws IOException {
        return new LuceneWriter(generation, dataFormat, baseDir, null, Codec.getDefault(), null);
    }

    private void addDoc(LuceneWriter writer, String id) throws IOException {
        MappedFieldType keywordField = mock(MappedFieldType.class);
        when(keywordField.typeName()).thenReturn("keyword");
        when(keywordField.name()).thenReturn("_id");
        when(keywordField.hasDocValues()).thenReturn(false);

        LuceneDocumentInput input = new LuceneDocumentInput();
        input.addField(keywordField, id);
        input.setRowId(LuceneDocumentInput.ROW_ID_FIELD, 0);
        writer.addDoc(input);
    }

    private LuceneWriter createWriterWithDoc(Path baseDir, long generation, String id) throws IOException {
        LuceneWriter writer = createWriter(baseDir, generation);
        addDoc(writer, id);
        return writer;
    }

    public void testGenerationMatchesWriter() throws IOException {
        Path baseDir = createTempDir();
        long gen = randomLongBetween(1, 100);
        try (LuceneWriter writer = new LuceneWriter(gen, dataFormat, baseDir, null, Codec.getDefault(), null)) {
            DeleterImpl<?> deleter = new DeleterImpl<>(writer);
            assertEquals("Deleter generation should match writer generation", gen, deleter.generation());
        }
    }

    public void testDeleteDocRemovesDocument() throws IOException {
        Path baseDir = createTempDir();
        try (LuceneWriter writer = createWriterWithDoc(baseDir, 1L, "doc-to-delete")) {
            DeleterImpl<?> deleter = new DeleterImpl<>(writer);

            DeleteInput deleteInput = new DeleteInput("_id", new BytesRef("doc-to-delete"), 1L);
            DeleteResult result = deleter.deleteDoc(deleteInput);

            assertTrue("deleteDoc should return Success", result instanceof DeleteResult.Success);

            // Flush and verify the doc is marked deleted
            FileInfos fileInfos = writer.flush();
            WriterFileSet wfs = fileInfos.getWriterFileSet(dataFormat).get();

            try (NIOFSDirectory dir = new NIOFSDirectory(Path.of(wfs.directory())); IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                Term uid = new Term("_id", "doc-to-delete");
                assertEquals("Deleted doc should not be found", 0, searcher.count(new TermQuery(uid)));
            }
        }
    }

    public void testDeleteDocReturnsSuccessForNonExistentDoc() throws IOException {
        Path baseDir = createTempDir();
        try (LuceneWriter writer = createWriterWithDoc(baseDir, 1L, "existing-doc")) {
            DeleterImpl<?> deleter = new DeleterImpl<>(writer);

            // Delete a doc that doesn't exist — Lucene doesn't error, just no-ops
            DeleteInput deleteInput = new DeleteInput("_id", new BytesRef("non-existent"), 1L);
            DeleteResult result = deleter.deleteDoc(deleteInput);

            assertTrue("deleteDoc should return Success even for non-existent doc", result instanceof DeleteResult.Success);
        }
    }
}
