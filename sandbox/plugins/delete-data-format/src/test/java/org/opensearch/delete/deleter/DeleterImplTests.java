/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.delete.deleter;

import org.apache.lucene.index.Term;
import org.opensearch.be.lucene.index.LuceneWriter;
import org.opensearch.composite.CompositeWriter;
import org.opensearch.index.engine.dataformat.DeleteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockDocumentInput;
import org.opensearch.index.engine.dataformat.stub.MockIndexingExecutionEngine;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeleterImplTests extends OpenSearchTestCase {

    public void testDeleteWithLuceneWriterDelegatesToLucene() throws IOException {
        LuceneWriter luceneWriter = mock(LuceneWriter.class);
        when(luceneWriter.generation()).thenReturn(5L);

        DeleterImpl deleter = new DeleterImpl(luceneWriter);
        Term uid = new Term("_id", "doc1");
        DeleteResult result = deleter.deleteDoc(uid, null);

        verify(luceneWriter).deleteDocument(uid);
        assertTrue(result instanceof DeleteResult.Success);
        assertTrue(deleter.getDeletedRowIds().isEmpty());
    }

    public void testDeleteWithCompositeWriterDelegatesToLucene() throws IOException {
        LuceneWriter luceneWriter = mock(LuceneWriter.class);
        CompositeWriter compositeWriter = mock(CompositeWriter.class);
        when(compositeWriter.generation()).thenReturn(3L);
        when(compositeWriter.findWriterByFormat("lucene")).thenAnswer(inv -> Optional.of(luceneWriter));

        DeleterImpl deleter = new DeleterImpl(compositeWriter);
        Term uid = new Term("_id", "doc2");
        deleter.deleteDoc(uid, null);

        verify(luceneWriter).deleteDocument(uid);
        assertTrue(deleter.getDeletedRowIds().isEmpty());
    }

    public void testDeleteWithNonLuceneWriterTracksRowId() throws IOException {
        MockIndexingExecutionEngine indexEngine = new MockIndexingExecutionEngine(new MockDataFormat());
        Writer<MockDocumentInput> writer = indexEngine.createWriter(1L);

        DeleterImpl deleter = new DeleterImpl(writer);
        Term uid = new Term("_id", "doc1");
        DeleteResult result = deleter.deleteDoc(uid, 42L);

        assertTrue(result instanceof DeleteResult.Success);
        assertEquals(1, deleter.getDeletedRowIds().size());
        assertTrue(deleter.getDeletedRowIds().contains(42L));
    }

    public void testMultipleDeletesAccumulateRowIds() throws IOException {
        MockIndexingExecutionEngine indexEngine = new MockIndexingExecutionEngine(new MockDataFormat());
        Writer<MockDocumentInput> writer = indexEngine.createWriter(1L);

        DeleterImpl deleter = new DeleterImpl(writer);
        deleter.deleteDoc(new Term("_id", "a"), 1L);
        deleter.deleteDoc(new Term("_id", "b"), 2L);
        deleter.deleteDoc(new Term("_id", "c"), 3L);

        assertEquals(3, deleter.getDeletedRowIds().size());
    }

    public void testGenerationMatchesWriter() {
        LuceneWriter luceneWriter = mock(LuceneWriter.class);
        when(luceneWriter.generation()).thenReturn(7L);

        DeleterImpl deleter = new DeleterImpl(luceneWriter);
        assertEquals(7L, deleter.generation());
    }

    public void testCompositeWriterWithoutLuceneFallsBackToRowId() throws IOException {
        CompositeWriter compositeWriter = mock(CompositeWriter.class);
        when(compositeWriter.generation()).thenReturn(2L);
        when(compositeWriter.findWriterByFormat("lucene")).thenAnswer(inv -> Optional.empty());

        DeleterImpl deleter = new DeleterImpl(compositeWriter);
        deleter.deleteDoc(new Term("_id", "doc1"), 10L);

        assertEquals(1, deleter.getDeletedRowIds().size());
        assertTrue(deleter.getDeletedRowIds().contains(10L));
    }
}
