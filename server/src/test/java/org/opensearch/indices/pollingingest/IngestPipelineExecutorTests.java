/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.ingest.IngestService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.IntConsumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class IngestPipelineExecutorTests extends OpenSearchTestCase {

    private IngestService ingestService;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ingestService = mock(IngestService.class);
    }

    // --- Construction tests ---

    public void testConstructorRequiresNonNullIngestService() {
        expectThrows(NullPointerException.class, () -> new IngestPipelineExecutor(null, "test_index", (String) null));
    }

    public void testConstructorRequiresNonNullIndex() {
        expectThrows(NullPointerException.class, () -> new IngestPipelineExecutor(ingestService, null, (String) null));
    }

    // --- Execution: no pipeline configured ---

    public void testExecutePipelines_NoPipeline_ReturnsSourceUnchanged() throws Exception {
        IngestPipelineExecutor executor = new IngestPipelineExecutor(ingestService, "test_index", (String) null);

        Map<String, Object> source = new HashMap<>();
        source.put("name", "alice");

        Map<String, Object> result = executor.executePipelines("1", source);

        assertSame(source, result);
        verify(ingestService, never()).executeBulkRequest(anyInt(), any(), any(), any(), any(), anyString());
    }

    // --- Execution: pipeline transforms source ---

    public void testExecutePipelines_TransformsSource() throws Exception {
        mockPipelineExecution("added_field", "added_value");
        IngestPipelineExecutor executor = new IngestPipelineExecutor(ingestService, "test_index", "my-pipeline");

        Map<String, Object> source = new HashMap<>();
        source.put("name", "alice");

        Map<String, Object> result = executor.executePipelines("1", source);

        assertNotNull(result);
        verify(ingestService).executeBulkRequest(anyInt(), any(), any(), any(), any(), anyString());
    }

    // --- Execution: pipeline drops document ---

    public void testExecutePipelines_DropsDocument() throws Exception {
        doAnswer(invocation -> {
            IntConsumer onDropped = invocation.getArgument(4);
            BiConsumer<Thread, Exception> onCompletion = invocation.getArgument(3);
            onDropped.accept(0);
            onCompletion.accept(Thread.currentThread(), null);
            return null;
        }).when(ingestService).executeBulkRequest(anyInt(), any(), any(), any(), any(), anyString());

        IngestPipelineExecutor executor = new IngestPipelineExecutor(ingestService, "test_index", "drop-pipeline");

        Map<String, Object> source = new HashMap<>();
        source.put("name", "alice");

        Map<String, Object> result = executor.executePipelines("1", source);

        assertNull(result);
    }

    // --- Execution: pipeline failure ---

    public void testExecutePipelines_Failure() {
        doAnswer(invocation -> {
            BiConsumer<Integer, Exception> onFailure = invocation.getArgument(2);
            BiConsumer<Thread, Exception> onCompletion = invocation.getArgument(3);
            onFailure.accept(0, new RuntimeException("processor failed"));
            onCompletion.accept(Thread.currentThread(), null);
            return null;
        }).when(ingestService).executeBulkRequest(anyInt(), any(), any(), any(), any(), anyString());

        IngestPipelineExecutor executor = new IngestPipelineExecutor(ingestService, "test_index", "fail-pipeline");

        Map<String, Object> source = new HashMap<>();
        source.put("name", "alice");

        Exception e = expectThrows(RuntimeException.class, () -> executor.executePipelines("1", source));
        assertTrue(e.getMessage().contains("Ingest pipeline execution failed"));
        assertTrue(e.getCause().getMessage().contains("processor failed"));
    }

    public void testExecutePipelines_CompletionException() {
        doAnswer(invocation -> {
            BiConsumer<Thread, Exception> onCompletion = invocation.getArgument(3);
            onCompletion.accept(Thread.currentThread(), new RuntimeException("bulk execution failed"));
            return null;
        }).when(ingestService).executeBulkRequest(anyInt(), any(), any(), any(), any(), anyString());

        IngestPipelineExecutor executor = new IngestPipelineExecutor(ingestService, "test_index", "fail-pipeline");

        Map<String, Object> source = new HashMap<>();
        source.put("name", "alice");

        RuntimeException e = expectThrows(RuntimeException.class, () -> executor.executePipelines("1", source));
        assertTrue(e.getMessage().contains("Ingest pipeline execution failed"));
    }

    // --- Guardrails ---

    public void testExecutePipelines_IdMutation_Throws() {
        doAnswer(invocation -> {
            Iterable<DocWriteRequest<?>> requests = invocation.getArgument(1);
            BiConsumer<Thread, Exception> onCompletion = invocation.getArgument(3);
            for (DocWriteRequest<?> req : requests) {
                ((IndexRequest) req).id("changed_id");
            }
            onCompletion.accept(Thread.currentThread(), null);
            return null;
        }).when(ingestService).executeBulkRequest(anyInt(), any(), any(), any(), any(), anyString());

        IngestPipelineExecutor executor = new IngestPipelineExecutor(ingestService, "test_index", "mutate-pipeline");

        Map<String, Object> source = new HashMap<>();
        source.put("name", "alice");

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> executor.executePipelines("1", source));
        assertTrue(e.getMessage().contains("_id mutations are not allowed"));
    }

    public void testExecutePipelines_RoutingMutation_Throws() {
        doAnswer(invocation -> {
            Iterable<DocWriteRequest<?>> requests = invocation.getArgument(1);
            BiConsumer<Thread, Exception> onCompletion = invocation.getArgument(3);
            for (DocWriteRequest<?> req : requests) {
                ((IndexRequest) req).routing("new_routing");
            }
            onCompletion.accept(Thread.currentThread(), null);
            return null;
        }).when(ingestService).executeBulkRequest(anyInt(), any(), any(), any(), any(), anyString());

        IngestPipelineExecutor executor = new IngestPipelineExecutor(ingestService, "test_index", "mutate-pipeline");

        Map<String, Object> source = new HashMap<>();
        source.put("name", "alice");

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> executor.executePipelines("1", source));
        assertTrue(e.getMessage().contains("_routing mutations are not allowed"));
    }

    // --- Helper ---

    private void mockPipelineExecution(String addedField, Object addedValue) {
        doAnswer(invocation -> {
            Iterable<DocWriteRequest<?>> requests = invocation.getArgument(1);
            BiConsumer<Thread, Exception> onCompletion = invocation.getArgument(3);
            for (DocWriteRequest<?> req : requests) {
                IndexRequest indexRequest = (IndexRequest) req;
                Map<String, Object> sourceMap = indexRequest.sourceAsMap();
                sourceMap.put(addedField, addedValue);
                indexRequest.source(sourceMap);
            }
            onCompletion.accept(Thread.currentThread(), null);
            return null;
        }).when(ingestService).executeBulkRequest(anyInt(), any(), any(), any(), any(), anyString());
    }
}
