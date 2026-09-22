/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link NoOpDeleteExecutionEngine}. */
public class NoOpDeleteExecutionEngineTests extends OpenSearchTestCase {

    private final NoOpDeleteExecutionEngine engine = NoOpDeleteExecutionEngine.INSTANCE;

    private Writer<?> writerOfGeneration(long generation) {
        Writer<?> writer = mock(Writer.class);
        when(writer.generation()).thenReturn(generation);
        return writer;
    }

    public void testDeleteDocumentRejectsOperation() {
        DeleteInput input = new DeleteInput(IdFieldMapper.NAME, "doc1", 1L);
        Writer<?> writer = writerOfGeneration(1L);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> engine.deleteDocument(input, writer));
        assertEquals(NoOpDeleteExecutionEngine.UNSUPPORTED_MESSAGE, e.getMessage());
    }

    public void testIndexingPathCallsAreInert() throws IOException {
        assertNull("no deleter is created for a writer", engine.createDeleter(writerOfGeneration(1L)));
        engine.recordWrite("doc1", new DocumentLocation(1L, 0L));
        assertFalse("checkout never reports applied deletes", engine.onWriterCheckedOut(1L));
        assertFalse("checkout of an unknown generation is a no-op", engine.onWriterCheckedOut(99L));
        assertEquals(0L, engine.ramBytesUsed());
        assertNull(engine.refresh(new RefreshInput(List.of(), List.of())));
        assertNull("no data format backs the no-op engine", engine.getDataFormat());
    }

    public void testCloseIsIdempotentOnSharedInstance() throws IOException {
        engine.close();
        engine.close();
        assertFalse(engine.onWriterCheckedOut(1L));
        expectThrows(IllegalArgumentException.class, () -> engine.deleteDocument(new DeleteInput(IdFieldMapper.NAME, "doc1", 1L), null));
    }
}
