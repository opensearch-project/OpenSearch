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
import org.opensearch.index.engine.dataformat.FlushAndCloseWriterException;
import org.opensearch.index.engine.dataformat.WriteResult;
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
        CompositeWriter writer = new CompositeWriter(compositeEngine, 0);
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
        CompositeWriter writer = new CompositeWriter(compositeEngine, 0);
        try {
            secondaryEngine.getLastCreatedWriter()
                .setResultToReturn(new WriteResult.Failure(new IOException("secondary write failed"), -1, -1, -1));
            WriteResult result = writer.addDoc(createDocumentInput());
            assertTrue(result instanceof WriteResult.Failure);
            assertTrue(((WriteResult.Failure) result).cause() instanceof FlushAndCloseWriterException);
            assertEquals(1, primaryEngine.getLastCreatedWriter().addDocCallCount.get());
            assertTrue(primaryEngine.getLastCreatedWriter().rollbackCalled);
        } finally {
            writer.close();
        }
    }

    public void testSecondaryRollbackFailureAbortsWriter() throws IOException {
        CompositeWriter writer = new CompositeWriter(compositeEngine, 0);
        try {
            secondaryEngine.getLastCreatedWriter()
                .setResultToReturn(new WriteResult.Failure(new IOException("secondary failed"), -1, -1, -1));
            primaryEngine.getLastCreatedWriter().rollbackFailure = new IOException("rollback failed");
            writer.addDoc(createDocumentInput());
            assertTrue(writer.isAborted());
            assertEquals(CompositeWriter.WriterState.ABORTED, writer.getState());
        } finally {
            writer.close();
        }
    }

    public void testAddDocThrowsWhenAborted() throws IOException {
        CompositeWriter writer = new CompositeWriter(compositeEngine, 0);
        try {
            writer.abort();
            expectThrows(IllegalStateException.class, () -> writer.addDoc(createDocumentInput()));
        } finally {
            writer.close();
        }
    }

    public void testFlushAfterAbortThrows() throws IOException {
        CompositeWriter writer = new CompositeWriter(compositeEngine, 0);
        try {
            writer.abort();
            expectThrows(IllegalStateException.class, writer::flush);
        } finally {
            writer.close();
        }
    }

    public void testPrimaryFlushFailurePropagates() throws IOException {
        CompositeWriter writer = new CompositeWriter(compositeEngine, 0);
        try {
            primaryEngine.getLastCreatedWriter().flushFailure = new IOException("primary flush failed");
            IOException e = expectThrows(IOException.class, writer::flush);
            assertEquals("primary flush failed", e.getMessage());
        } finally {
            writer.close();
        }
    }

    public void testSecondaryFlushFailurePropagates() throws IOException {
        CompositeWriter writer = new CompositeWriter(compositeEngine, 0);
        try {
            secondaryEngine.getLastCreatedWriter().flushFailure = new IOException("secondary flush failed");
            IOException e = expectThrows(IOException.class, writer::flush);
            assertEquals("secondary flush failed", e.getMessage());
        } finally {
            writer.close();
        }
    }

    private CompositeDocumentInput createDocumentInput() {
        DocumentInput<?> primaryInput = new MockDocumentInput();
        Map<DataFormat, DocumentInput<?>> secondaryInputs = Map.of(secondaryEngine.getDataFormat(), new MockDocumentInput());
        return new CompositeDocumentInput(primaryEngine.getDataFormat(), primaryInput, secondaryInputs);
    }
}
