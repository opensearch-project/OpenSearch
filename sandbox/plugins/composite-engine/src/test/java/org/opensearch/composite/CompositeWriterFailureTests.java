/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.dataformat.WriterState;
import org.opensearch.index.engine.dataformat.stub.MockDocumentInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

/**
 * Unit tests for {@link CompositeWriter} failure handling: primary/secondary write failures,
 * rollback behavior, abort state transitions, and flush failure propagation.
 */
public class CompositeWriterFailureTests extends OpenSearchTestCase {

    private CompositeTestHelper.FailableEngine primaryEngine;
    private CompositeTestHelper.FailableEngine secondaryEngine;
    private CompositeTestHelper.FailableCommitter committer;
    private CompositeIndexingExecutionEngine compositeEngine;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        primaryEngine = new CompositeTestHelper.FailableEngine("lucene");
        secondaryEngine = new CompositeTestHelper.FailableEngine("parquet");
        committer = new CompositeTestHelper.FailableCommitter();
        compositeEngine = CompositeTestHelper.buildFailableEngine(primaryEngine, committer, secondaryEngine);
    }

    public void testPrimaryWriteFailureReturnsFailureImmediately() throws IOException {
        CompositeWriter writer = new CompositeWriter(compositeEngine, new WriterConfig(0));
        try {
            primaryEngine.getLastCreatedWriter()
                .setResultToReturn(new WriteResult.Failure(new IOException("primary disk full"), -1, -1, -1));
            WriteResult result = writer.addDoc(createDocumentInput());
            assertTrue(result instanceof WriteResult.Failure);
            assertEquals(0, secondaryEngine.getLastCreatedWriter().addDocCallCount.get());
        } finally {
            writer.close();
        }
    }

    public void testSecondaryWriteFailureRollsBackPrimary() throws IOException {
        CompositeWriter writer = new CompositeWriter(compositeEngine, new WriterConfig(0));
        try {
            IOException secondaryCause = new IOException("secondary write failed");
            secondaryEngine.getLastCreatedWriter().setResultToReturn(new WriteResult.Failure(secondaryCause, -1, -1, -1));
            WriteResult result = writer.addDoc(createDocumentInput());
            assertTrue(result instanceof WriteResult.Failure);
            // Rollback succeeds, original cause is propagated (no signal exception wrapping).
            assertSame(secondaryCause, ((WriteResult.Failure) result).cause());
            assertEquals(1, primaryEngine.getLastCreatedWriter().addDocCallCount.get());
            assertTrue(primaryEngine.getLastCreatedWriter().rollbackCalled);
            // FailableWriter mimics Lucene-style strategy: rollback success → RETIRED_FLUSHABLE.
            // Composite aggregates worst child state.
            assertEquals(WriterState.RETIRED_FLUSHABLE, writer.state());
        } finally {
            writer.close();
        }
    }

    public void testPrimaryFlushFailurePropagates() throws IOException {
        CompositeWriter writer = new CompositeWriter(compositeEngine, new WriterConfig(0));
        try {
            primaryEngine.getLastCreatedWriter().flushFailure = new IOException("primary flush failed");
            IOException e = expectThrows(IOException.class, () -> writer.flush(org.opensearch.index.engine.dataformat.FlushInput.EMPTY));
            assertEquals("primary flush failed", e.getMessage());
        } finally {
            writer.close();
        }
    }

    public void testSecondaryFlushFailurePropagates() throws IOException {
        CompositeWriter writer = new CompositeWriter(compositeEngine, new WriterConfig(0));
        try {
            secondaryEngine.getLastCreatedWriter().flushFailure = new IOException("secondary flush failed");
            IOException e = expectThrows(IOException.class, () -> writer.flush(org.opensearch.index.engine.dataformat.FlushInput.EMPTY));
            assertEquals("secondary flush failed", e.getMessage());
        } finally {
            writer.close();
        }
    }

    // ──────────────────────────────────────────────────────────────────────────
    // State transitions
    // ──────────────────────────────────────────────────────────────────────────

    public void testStateActiveAfterSuccessfulAddDoc() throws IOException {
        CompositeWriter writer = new CompositeWriter(compositeEngine, new WriterConfig(0));
        try {
            WriteResult result = writer.addDoc(createDocumentInput());
            assertTrue("expected success", result instanceof WriteResult.Success);
            assertEquals(WriterState.ACTIVE, writer.state());
        } finally {
            writer.close();
        }
    }

    public void testAddDocAfterRetiredFlushableThrows() throws IOException {
        CompositeWriter writer = new CompositeWriter(compositeEngine, new WriterConfig(0));
        try {
            primaryEngine.getLastCreatedWriter().setState(WriterState.RETIRED_FLUSHABLE);
            expectThrows(IllegalStateException.class, () -> writer.addDoc(createDocumentInput()));
        } finally {
            writer.close();
        }
    }

    public void testAddDocAfterCloseThrows() throws IOException {
        CompositeWriter writer = new CompositeWriter(compositeEngine, new WriterConfig(0));
        writer.close();
        expectThrows(IllegalStateException.class, () -> writer.addDoc(createDocumentInput()));
    }

    public void testRollbackToOnCompositeIsUnsupported() throws IOException {
        // CompositeWriter does not override Writer.rollbackTo — calling it externally
        // should hit the interface default which throws UnsupportedOperationException.
        // Cross-format rollback is handled internally during a failed addDoc.
        CompositeWriter writer = new CompositeWriter(compositeEngine, new WriterConfig(0));
        try {
            expectThrows(UnsupportedOperationException.class, () -> writer.rollbackTo(0));
        } finally {
            writer.close();
        }
    }

    public void testRollbackHappensOncePerFailedAddDoc() throws IOException {
        CompositeWriter writer = new CompositeWriter(compositeEngine, new WriterConfig(0));
        try {
            // Secondary fails on the first addDoc → composite rolls back primary once.
            secondaryEngine.getLastCreatedWriter()
                .setResultToReturn(new WriteResult.Failure(new IOException("secondary failed"), -1, -1, -1));
            WriteResult result = writer.addDoc(createDocumentInput());
            assertTrue(result instanceof WriteResult.Failure);

            // Primary's rollback was called exactly once for this failed addDoc — not twice.
            assertTrue("primary rollback should have been called", primaryEngine.getLastCreatedWriter().rollbackCalled);
            // Composite is now RETIRED_FLUSHABLE (Lucene-style sub-writer); the next addDoc
            // must throw IllegalStateException because state != ACTIVE. Importantly, this
            // means there is NO second call into primaryWriter.rollbackLastDoc — once a
            // rollback has run, the writer is retired and cannot accept another addDoc that
            // could trigger a second rollback.
            assertEquals(WriterState.RETIRED_FLUSHABLE, writer.state());
            expectThrows(IllegalStateException.class, () -> writer.addDoc(createDocumentInput()));
        } finally {
            writer.close();
        }
    }

    public void testPrimaryFailureLeavesSecondariesUntouched() throws IOException {
        CompositeWriter writer = new CompositeWriter(compositeEngine, new WriterConfig(0));
        try {
            primaryEngine.getLastCreatedWriter().setResultToReturn(new WriteResult.Failure(new IOException("primary failed"), -1, -1, -1));
            WriteResult result = writer.addDoc(createDocumentInput());
            assertTrue(result instanceof WriteResult.Failure);

            // No secondary writer should have seen the doc.
            assertEquals(0, secondaryEngine.getLastCreatedWriter().addDocCallCount.get());
            // Primary's rollback was invoked by CompositeWriter so the leaf can decide whether
            // it has anything to undo (idempotent inside the leaf). FailableWriter records the
            // call regardless of whether work was done.
            assertTrue("primary rollback should have been called by CompositeWriter", primaryEngine.getLastCreatedWriter().rollbackCalled);
        } finally {
            writer.close();
        }
    }

    public void testCloseFromRetiredFlushableTransitionsToClosed() throws IOException {
        CompositeWriter writer = new CompositeWriter(compositeEngine, new WriterConfig(0));
        primaryEngine.getLastCreatedWriter().setState(WriterState.RETIRED_FLUSHABLE);
        assertEquals(WriterState.RETIRED_FLUSHABLE, writer.state());
        writer.close();
        assertEquals(WriterState.CLOSED, writer.state());
    }

    private CompositeDocumentInput createDocumentInput() {
        return createDocumentInput(0L);
    }

    private CompositeDocumentInput createDocumentInput(long rowId) {
        DocumentInput<?> primaryInput = new MockDocumentInput();
        Map<DataFormat, DocumentInput<?>> secondaryInputs = Map.of(secondaryEngine.getDataFormat(), new MockDocumentInput());
        CompositeDocumentInput composite = new CompositeDocumentInput(primaryEngine.getDataFormat(), primaryInput, secondaryInputs);
        composite.setRowId(DocumentInput.ROW_ID_FIELD, rowId);
        return composite;
    }
}
