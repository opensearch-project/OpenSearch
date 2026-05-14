/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Optional;

/**
 * Tests for {@link CompositeWriter}.
 */
public class CompositeWriterTests extends OpenSearchTestCase {

    private CompositeIndexingExecutionEngine engine;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        engine = CompositeTestHelper.createStubEngine("lucene", "parquet");
    }

    public void testWriterGenerationIsPreserved() throws IOException {
        long gen = randomLongBetween(0, 1000);
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(gen));
        assertEquals(gen, writer.getWriterGeneration());
        writer.close();
    }

    public void testAbortedDefaultsToFalse() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        assertFalse(writer.isAborted());
        assertEquals(CompositeWriter.WriterState.ACTIVE, writer.getState());
        writer.close();
    }

    public void testAbortSetsAbortedFlag() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        writer.abort();
        assertTrue(writer.isAborted());
        assertEquals(CompositeWriter.WriterState.ABORTED, writer.getState());
        writer.close();
    }

    public void testFlushPendingDefaultsToFalse() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        assertFalse(writer.isFlushPending());
        assertEquals(CompositeWriter.WriterState.ACTIVE, writer.getState());
        writer.close();
    }

    public void testSetFlushPendingSetsFlag() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        writer.setFlushPending();
        assertTrue(writer.isFlushPending());
        assertEquals(CompositeWriter.WriterState.FLUSH_PENDING, writer.getState());
        writer.close();
    }

    public void testAbortDoesNotTransitionFromFlushPending() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        writer.setFlushPending();
        expectThrows(IllegalStateException.class, writer::abort);
        assertTrue(writer.isFlushPending());
        assertEquals(CompositeWriter.WriterState.FLUSH_PENDING, writer.getState());
        writer.close();
    }

    public void testFlushPendingDoesNotTransitionFromAborted() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        writer.abort();
        expectThrows(IllegalStateException.class, writer::setFlushPending);
        assertTrue(writer.isAborted());
        assertEquals(CompositeWriter.WriterState.ABORTED, writer.getState());
        writer.close();
    }

    public void testFlushReturnsFileInfos() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        FileInfos fileInfos = writer.flush();
        assertNotNull(fileInfos);
        writer.close();
    }

    public void testSyncDoesNotThrow() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        writer.sync();
        writer.close();
    }

    public void testCloseDoesNotThrow() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        writer.close();
        // calling close again should also not throw
        writer.close();
    }

    public void testMappingVersionSetAtConstruction() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        assertEquals(0L, writer.mappingVersion());
        writer.close();
    }

    public void testUpdateMappingVersionPropagates() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        writer.updateMappingVersion(5L);
        assertEquals(5L, writer.mappingVersion());
        writer.close();
    }

    public void testIsSchemaMutableReturnsFalseWhenAnySubWriterImmutable() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        assertTrue(writer.isSchemaMutable());

        // Make the primary sub-writer immutable
        CompositeTestHelper.StubIndexingExecutionEngine primaryEngine = (CompositeTestHelper.StubIndexingExecutionEngine) engine
            .getPrimaryDelegate();
        primaryEngine.lastCreatedWriter.setSchemaMutable(false);
        assertFalse(writer.isSchemaMutable());

        writer.close();
    }

    public void testGetWriterForFormatReturnsLuceneWriter() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, 1L);
        Optional<Writer<?>> result = writer.getWriterForFormat("lucene");

        assertTrue("Should return present for 'lucene'", result.isPresent());
        assertEquals("Returned writer generation should match", 1L, result.get().generation());
    }

    public void testGetWriterForFormatReturnsParquetWriter() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, 1L);
        Optional<Writer<?>> result = writer.getWriterForFormat("parquet");

        assertTrue("Should return present for 'parquet'", result.isPresent());
        assertEquals("Returned writer generation should match", 1L, result.get().generation());
    }

    public void testGetWriterForFormatReturnsEmptyForUnknownFormat() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, 1L);
        Optional<Writer<?>> result = writer.getWriterForFormat("unknown");

        assertFalse("Should return empty for unknown format", result.isPresent());
        assertEquals("Writer generation should still be accessible", 1L, writer.generation());
    }
}
