/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

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
        CompositeWriter writer = new CompositeWriter(engine, gen);
        assertEquals(gen, writer.getWriterGeneration());
        writer.close();
    }

    public void testAbortedDefaultsToFalse() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, 0);
        assertFalse(writer.isAborted());
        assertEquals(CompositeWriter.WriterState.ACTIVE, writer.getState());
        writer.close();
    }

    public void testAbortSetsAbortedFlag() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, 0);
        writer.abort();
        assertTrue(writer.isAborted());
        assertEquals(CompositeWriter.WriterState.ABORTED, writer.getState());
        writer.close();
    }

    public void testFlushPendingDefaultsToFalse() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, 0);
        assertFalse(writer.isFlushPending());
        assertEquals(CompositeWriter.WriterState.ACTIVE, writer.getState());
        writer.close();
    }

    public void testSetFlushPendingSetsFlag() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, 0);
        writer.setFlushPending();
        assertTrue(writer.isFlushPending());
        assertEquals(CompositeWriter.WriterState.FLUSH_PENDING, writer.getState());
        writer.close();
    }

    public void testAbortDoesNotTransitionFromFlushPending() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, 0);
        writer.setFlushPending();
        expectThrows(IllegalStateException.class, writer::abort);
        assertTrue(writer.isFlushPending());
        assertEquals(CompositeWriter.WriterState.FLUSH_PENDING, writer.getState());
        writer.close();
    }

    public void testFlushPendingDoesNotTransitionFromAborted() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, 0);
        writer.abort();
        expectThrows(IllegalStateException.class, writer::setFlushPending);
        assertTrue(writer.isAborted());
        assertEquals(CompositeWriter.WriterState.ABORTED, writer.getState());
        writer.close();
    }

    public void testLockAndUnlock() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, 0);
        writer.lock();
        assertTrue(writer.tryLock());
        writer.unlock();
        writer.unlock();
        writer.close();
    }

    public void testTryLockSucceedsWhenUnlocked() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, 0);
        assertTrue(writer.tryLock());
        writer.unlock();
        writer.close();
    }

    public void testFlushReturnsFileInfos() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, 0);
        FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
        assertNotNull(fileInfos);
        writer.close();
    }

    public void testSyncDoesNotThrow() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, 0);
        writer.sync();
        writer.close();
    }

    public void testCloseDoesNotThrow() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, 0);
        writer.close();
        // calling close again should also not throw
        writer.close();
    }

}
