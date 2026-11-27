/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import org.opensearch.common.metrics.OperationStats;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipelineTests extends OpenSearchTestCase {

    public void testCreatePipelineWithEmptyConfig() {
        final Pipeline pipeline = Pipeline.createSystemIngestPipeline("test-index", Collections.emptyMap(), Collections.emptyMap());

        assertNotNull(pipeline);
        assertEquals("test-index_system_generated_ingest_pipeline", pipeline.getId());
        assertTrue(pipeline.getProcessors().isEmpty());
    }

    public void testCreatePipelineWithOneProcessor() throws Exception {
        final Processor processor = mock(Processor.class);
        final Processor.Factory factory = mock(Processor.Factory.class);
        final Map<String, Object> config = Map.of("key", "value");
        when(processor.isSystemGenerated()).thenReturn(true);
        when(factory.isSystemGenerated()).thenReturn(true);
        when(factory.create(any(), any(), any(), any())).thenReturn(processor);

        final Pipeline pipeline = Pipeline.createSystemIngestPipeline("my-index", Map.of("factory", factory), config);

        assertNotNull(pipeline);
        assertEquals("my-index_system_generated_ingest_pipeline", pipeline.getId());
        assertEquals(1, pipeline.getProcessors().size());
        assertSame(processor, pipeline.getProcessors().get(0));

        verify(factory, times(1)).create(any(), any(), any(), any());
    }

    public void testCreatePipelineWithFactoryException() throws Exception {
        final Map<String, Object> config = Map.of("key", "value");
        final Processor.Factory faultyFactory = mock(Processor.Factory.class);
        when(faultyFactory.create(any(), any(), any(), any())).thenThrow(new RuntimeException("Factory failed"));

        final RuntimeException e = assertThrows(
            RuntimeException.class,
            () -> Pipeline.createSystemIngestPipeline("my-index", Map.of("factory", faultyFactory), config)
        );
        assertTrue(e.getMessage().contains("Factory failed"));
    }

    public void testTerminateRequestEndsPipelineAndIsClearedOnCompletion() {
        TestProcessor first = new TestProcessor(doc -> {});
        TestProcessor terminator = terminator();
        TestProcessor last = new TestProcessor(doc -> {});
        Pipeline pipeline = new Pipeline("_id", null, null, new CompoundProcessor(first, terminator, last));
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());

        IngestDocument[] result = new IngestDocument[1];
        Exception[] failure = new Exception[1];
        pipeline.execute(ingestDocument, (r, e) -> {
            result[0] = r;
            failure[0] = e;
        });

        assertNull(failure[0]);
        assertSame(ingestDocument, result[0]);
        assertEquals(1, first.getInvokedCounter());
        assertEquals(1, terminator.getInvokedCounter());
        assertEquals(0, last.getInvokedCounter());
        // the request ends with the pipeline, so that a calling pipeline or a later pipeline is not affected
        assertFalse(ingestDocument.isPipelineTerminated());
        // terminating is a success, not a failure
        OperationStats stats = pipeline.getMetrics().createStats();
        assertEquals(1, stats.getCount());
        assertEquals(0, stats.getFailedCount());
        assertEquals(0, stats.getCurrent());
    }

    public void testTerminateRequestIsClearedWhenPipelineFails() {
        // a processor that asks for termination and then fails must not leave the request behind on the document
        TestProcessor terminateThenFail = new TestProcessor(doc -> {
            doc.terminatePipeline();
            throw new IllegalStateException("boom");
        });
        Pipeline pipeline = new Pipeline("_id", null, null, new CompoundProcessor(terminateThenFail));
        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());

        Exception[] failure = new Exception[1];
        pipeline.execute(ingestDocument, (r, e) -> failure[0] = e);

        assertNotNull(failure[0]);
        assertFalse(ingestDocument.isPipelineTerminated());
        assertEquals(1, pipeline.getMetrics().createStats().getFailedCount());
    }

    public void testBatchExecuteTerminatedDocumentsAreSuccessfulAndRequestIsCleared() {
        TestProcessor first = new TestProcessor("terminator", "terminate", null, doc -> {
            if (doc.hasField(IngestDocumentPreparer.SHOULD_FAIL_KEY)) {
                doc.terminatePipeline();
            }
        });
        TestProcessor second = new TestProcessor(doc -> {});
        Pipeline pipeline = new Pipeline("_id", null, null, new CompoundProcessor(first, second));
        List<IngestDocumentWrapper> wrappers = Arrays.asList(
            IngestDocumentPreparer.createIngestDocumentWrapper(1),
            IngestDocumentPreparer.createIngestDocumentWrapper(2, true),
            IngestDocumentPreparer.createIngestDocumentWrapper(3, true)
        );

        List<IngestDocumentWrapper> results = Collections.synchronizedList(new ArrayList<>());
        pipeline.batchExecute(wrappers, results::addAll);

        assertEquals(3, results.size());
        assertEquals(1, second.getInvokedCounter());
        for (IngestDocumentWrapper result : results) {
            assertNull(result.getException());
            assertNotNull(result.getIngestDocument());
            assertFalse(result.getIngestDocument().isPipelineTerminated());
        }
        OperationStats stats = pipeline.getMetrics().createStats();
        assertEquals(3, stats.getCount());
        assertEquals(0, stats.getFailedCount());
        assertEquals(0, stats.getCurrent());
    }

    private static TestProcessor terminator() {
        return new TestProcessor("terminator", "terminate", null, doc -> { doc.terminatePipeline(); });
    }
}
