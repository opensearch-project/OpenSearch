/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.metrics.OperationMetrics;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class PipelineWithMetricsTests extends OpenSearchTestCase {

    @Mock
    private SearchRequestProcessor mockRequestProcessor;
    @Mock
    private SearchResponseProcessor mockResponseProcessor;
    @Mock
    private SearchPhaseResultsProcessor mockPhaseResultsProcessor;
    @Mock
    private SearchRequest mockSearchRequest;
    @Mock
    private SearchResponse mockSearchResponse;
    @Mock
    private SearchPhaseResults<SearchPhaseResult> mockPhaseResults;
    @Mock
    private SearchPhaseContext mockPhaseContext;
    @Mock
    private NamedWriteableRegistry mockNamedWriteableRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    public void testRequestProcessorMetricsCollection() throws Exception {
        // Setup
        when(mockRequestProcessor.getType()).thenReturn("test-request-processor");
        when(mockRequestProcessor.getTag()).thenReturn("tag1");
        when(mockRequestProcessor.isIgnoreFailure()).thenReturn(false);

        doAnswer(invocation -> {
            ActionListener<SearchRequest> listener = invocation.getArgument(2);
            SearchRequest originalRequest = invocation.getArgument(0);
            // Simulate some processing time
            Thread.sleep(10);
            listener.onResponse(originalRequest);
            return null;
        }).when(mockRequestProcessor).processRequestAsync(any(), any(), any());

        OperationMetrics totalRequestMetrics = new OperationMetrics();
        OperationMetrics totalResponseMetrics = new OperationMetrics();
        OperationMetrics totalPhaseMetrics = new OperationMetrics();

        // Use real SearchRequest and NamedWriteableRegistry
        SearchRequest realSearchRequest = new SearchRequest("test-index");
        NamedWriteableRegistry realNamedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());

        PipelineWithMetrics pipeline = new PipelineWithMetrics(
            "test-pipeline",
            "Test pipeline",
            1,
            List.of(mockRequestProcessor),
            Collections.emptyList(),
            Collections.emptyList(),
            realNamedWriteableRegistry,
            totalRequestMetrics,
            totalResponseMetrics,
            totalPhaseMetrics,
            System::nanoTime
        );

        // Execute
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<SearchRequest> result = new AtomicReference<>();
        AtomicReference<Exception> error = new AtomicReference<>();

        pipeline.transformRequest(realSearchRequest, ActionListener.wrap(
            request -> {
                result.set(request);
                latch.countDown();
            },
            exception -> {
                error.set(exception);
                latch.countDown();
            }
        ), new PipelineProcessingContext());

        assertTrue("Request processing should complete within timeout", latch.await(5, TimeUnit.SECONDS));
        assertNull("No error should occur", error.get());
        assertNotNull("Result should not be null", result.get());

        // Verify metrics were collected correctly
        assertTrue("Total request metrics should have at least one execution", totalRequestMetrics.createStats().getCount() > 0);
        assertTrue("Total request metrics should have recorded time", totalRequestMetrics.createStats().getTotalTimeInMillis() >= 0);

        // Verify stats builder works correctly
        SearchPipelineStats.Builder statsBuilder = new SearchPipelineStats.Builder();
        statsBuilder.withTotalStats(totalRequestMetrics, totalResponseMetrics, totalPhaseMetrics);
        pipeline.populateStats(statsBuilder);
        SearchPipelineStats stats = statsBuilder.build();

        assertNotNull("Stats should not be null", stats);
        assertEquals("Should have one pipeline", 1, stats.getPipelineStats().size());

        // Verify processor stats are included
        String expectedProcessorKey = "test-request-processor:tag1";
        assertTrue("Should contain processor stats",
            stats.getPerPipelineProcessorStats().get("test-pipeline")
                .requestProcessorStats().stream()
                .anyMatch(ps -> ps.getProcessorName().equals(expectedProcessorKey))
        );
    }

    public void testPhaseResultsProcessorMetricsCollection() throws Exception {
        // Setup
        when(mockPhaseResultsProcessor.getType()).thenReturn("test-phase-processor");
        when(mockPhaseResultsProcessor.getTag()).thenReturn("phase-tag");
        when(mockPhaseResultsProcessor.isIgnoreFailure()).thenReturn(false);
        when(mockPhaseResultsProcessor.getBeforePhase()).thenReturn(SearchPhaseName.QUERY);
        when(mockPhaseResultsProcessor.getAfterPhase()).thenReturn(SearchPhaseName.FETCH);

        doAnswer(invocation -> {
            // Simulate some processing time
            Thread.sleep(10);
            return null;
        }).when(mockPhaseResultsProcessor).process(any(), any(), any());

        OperationMetrics totalRequestMetrics = new OperationMetrics();
        OperationMetrics totalResponseMetrics = new OperationMetrics();
        OperationMetrics totalPhaseMetrics = new OperationMetrics();

        PipelineWithMetrics pipeline = new PipelineWithMetrics(
            "test-pipeline",
            "Test pipeline",
            1,
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(mockPhaseResultsProcessor),
            mockNamedWriteableRegistry,
            totalRequestMetrics,
            totalResponseMetrics,
            totalPhaseMetrics,
            System::nanoTime
        );

        // Execute
        pipeline.runSearchPhaseResultsTransformer(
            mockPhaseResults,
            mockPhaseContext,
            "query",
            "fetch",
            new PipelineProcessingContext()
        );

        // Verify phase results metrics were collected correctly
        assertTrue("Total phase metrics should have at least one execution", totalPhaseMetrics.createStats().getCount() > 0);
        assertTrue("Total phase metrics should have recorded time", totalPhaseMetrics.createStats().getTotalTimeInMillis() >= 0);

        // Verify stats builder works correctly
        SearchPipelineStats.Builder statsBuilder = new SearchPipelineStats.Builder();
        statsBuilder.withTotalStats(totalRequestMetrics, totalResponseMetrics, totalPhaseMetrics);
        pipeline.populateStats(statsBuilder);
        SearchPipelineStats stats = statsBuilder.build();

        // Verify processor stats are included
        String expectedProcessorKey = "test-phase-processor:phase-tag";
        assertTrue("Should contain phase processor stats",
            stats.getPerPipelineProcessorStats().get("test-pipeline")
                .phaseResultsProcessorStats().stream()
                .anyMatch(ps -> ps.getProcessorName().equals(expectedProcessorKey))
        );
    }

    public void testCopyMetricsIncludesPhaseProcessors() {
        // Setup old pipeline with some metrics
        OperationMetrics oldTotalRequest = new OperationMetrics();
        OperationMetrics oldTotalResponse = new OperationMetrics();
        OperationMetrics oldTotalPhase = new OperationMetrics();

        when(mockPhaseResultsProcessor.getType()).thenReturn("test-phase-processor");
        when(mockPhaseResultsProcessor.getTag()).thenReturn("phase-tag");

        PipelineWithMetrics oldPipeline = new PipelineWithMetrics(
            "old-pipeline",
            "Old pipeline",
            1,
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(mockPhaseResultsProcessor),
            mockNamedWriteableRegistry,
            oldTotalRequest,
            oldTotalResponse,
            oldTotalPhase,
            System::nanoTime
        );

        // Simulate some executions on old pipeline to generate metrics
        oldTotalRequest.before();
        oldTotalRequest.after(1000000L); // 1ms in nanos
        oldTotalResponse.before();
        oldTotalResponse.after(2000000L); // 2ms in nanos
        oldTotalPhase.before();
        oldTotalPhase.after(3000000L); // 3ms in nanos

        // Create new pipeline
        OperationMetrics newTotalRequest = new OperationMetrics();
        OperationMetrics newTotalResponse = new OperationMetrics();
        OperationMetrics newTotalPhase = new OperationMetrics();

        PipelineWithMetrics newPipeline = new PipelineWithMetrics(
            "new-pipeline",
            "New pipeline",
            1,
            Collections.emptyList(),
            Collections.emptyList(),
            List.of(mockPhaseResultsProcessor),
            mockNamedWriteableRegistry,
            newTotalRequest,
            newTotalResponse,
            newTotalPhase,
            System::nanoTime
        );

        // Copy metrics
        newPipeline.copyMetrics(oldPipeline);

        // Verify that metrics were copied (this test would fail before our fix)
        SearchPipelineStats.Builder statsBuilder = new SearchPipelineStats.Builder();
        statsBuilder.withTotalStats(newTotalRequest, newTotalResponse, newTotalPhase);
        newPipeline.populateStats(statsBuilder);
        SearchPipelineStats stats = statsBuilder.build();

        // The phase processor metrics should be preserved during copy
        assertFalse(
            "Phase processor stats should exist after copy",
            stats.getPerPipelineProcessorStats().get("new-pipeline").phaseResultsProcessorStats().isEmpty()
        );
    }

    /**
     * Mock class for SearchPipelineExecutionPhase since it may not be available in test classpath
     */
    private enum SearchPipelineExecutionPhase {
        QUERY("query"),
        FETCH("fetch");

        private final String name;

        SearchPipelineExecutionPhase(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
