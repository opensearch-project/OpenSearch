/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseResults;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.Nullable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.search.SearchPhaseResult;

import java.util.Collections;
import java.util.List;
import java.util.function.LongSupplier;

/**
 * Concrete representation of a search pipeline, holding multiple processors.
 */
class Pipeline {

    public static final String REQUEST_PROCESSORS_KEY = "request_processors";
    public static final String RESPONSE_PROCESSORS_KEY = "response_processors";
    public static final String PHASE_PROCESSORS_KEY = "phase_results_processors";

    private static final Logger logger = LogManager.getLogger(Pipeline.class);

    private final String id;
    private final String description;
    private final Integer version;

    // TODO: Refactor org.opensearch.ingest.CompoundProcessor to implement our generic Processor interface
    // Then these can be CompoundProcessors instead of lists.
    private final List<SearchRequestProcessor> searchRequestProcessors;
    private final List<SearchResponseProcessor> searchResponseProcessors;
    private final List<SearchPhaseResultsProcessor> searchPhaseResultsProcessors;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final LongSupplier relativeTimeSupplier;

    Pipeline(
        String id,
        @Nullable String description,
        @Nullable Integer version,
        List<SearchRequestProcessor> requestProcessors,
        List<SearchResponseProcessor> responseProcessors,
        List<SearchPhaseResultsProcessor> phaseResultsProcessors,
        NamedWriteableRegistry namedWriteableRegistry,
        LongSupplier relativeTimeSupplier
    ) {
        this.id = id;
        this.description = description;
        this.version = version;
        this.searchRequestProcessors = Collections.unmodifiableList(requestProcessors);
        this.searchResponseProcessors = Collections.unmodifiableList(responseProcessors);
        this.searchPhaseResultsProcessors = Collections.unmodifiableList(phaseResultsProcessors);
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.relativeTimeSupplier = relativeTimeSupplier;
    }

    String getId() {
        return id;
    }

    String getDescription() {
        return description;
    }

    Integer getVersion() {
        return version;
    }

    List<SearchRequestProcessor> getSearchRequestProcessors() {
        return searchRequestProcessors;
    }

    List<SearchResponseProcessor> getSearchResponseProcessors() {
        return searchResponseProcessors;
    }

    List<SearchPhaseResultsProcessor> getSearchPhaseResultsProcessors() {
        return searchPhaseResultsProcessors;
    }

    protected void beforeTransformRequest() {}

    protected void afterTransformRequest(long timeInNanos) {}

    protected void onTransformRequestFailure() {}

    protected void beforeRequestProcessor(Processor processor) {}

    protected void afterRequestProcessor(Processor processor, long timeInNanos) {}

    protected void onRequestProcessorFailed(Processor processor) {}

    protected void beforeTransformResponse() {}

    protected void afterTransformResponse(long timeInNanos) {}

    protected void onTransformResponseFailure() {}

    protected void beforeResponseProcessor(Processor processor) {}

    protected void afterResponseProcessor(Processor processor, long timeInNanos) {}

    protected void onResponseProcessorFailed(Processor processor) {}

    void transformRequest(PipelinedRequest pipelinedRequest, ActionListener<SearchRequest> requestListener)
        throws SearchPipelineProcessingException {

        if (searchRequestProcessors.isEmpty()) {
            requestListener.onResponse(pipelinedRequest);
            return;
        }

        PipelineProcessingContext requestContext = pipelinedRequest.getPipelineProcessingContext();

        ActionListener<SearchRequest> finalListener = getTerminalSearchRequestActionListener(requestListener);

        // Chain listeners back-to-front
        ActionListener<SearchRequest> currentListener = finalListener;
        for (int i = searchRequestProcessors.size() - 1; i >= 0; i--) {
            final ActionListener<SearchRequest> nextListener = currentListener;
            // Conditionally wrap the current processor with a TrackingSearchRequestProcessorWrapper
            // if verbosePipeline mode is enabled. This allows detailed execution tracking for debugging purposes.
            final SearchRequestProcessor processor = pipelinedRequest.source().verbosePipeline()
                ? new TrackingSearchRequestProcessorWrapper(searchRequestProcessors.get(i))
                : searchRequestProcessors.get(i);
            currentListener = ActionListener.wrap(r -> {
                long start = relativeTimeSupplier.getAsLong();
                beforeRequestProcessor(processor);
                processor.processRequestAsync(r, requestContext, ActionListener.wrap(rr -> {
                    long took = relativeTimeSupplier.getAsLong() - start;
                    afterRequestProcessor(processor, took);
                    nextListener.onResponse(rr);
                }, e -> {
                    long took = relativeTimeSupplier.getAsLong() - start;
                    afterRequestProcessor(processor, took);
                    onRequestProcessorFailed(processor);
                    // When verbosePipeline is enabled, all processor failures are ignored to ensure the execution chain continues without
                    // interruption.TrackingSearchResponseProcessorWrapper will log all errors in detail for debugging purposes
                    if (processor.isIgnoreFailure() || r.source().verbosePipeline()) {
                        logger.warn(
                            "The exception from request processor ["
                                + processor.getType()
                                + "] in the search pipeline ["
                                + id
                                + "] was ignored",
                            e
                        );
                        nextListener.onResponse(r);
                    } else {
                        nextListener.onFailure(new SearchPipelineProcessingException(e));
                    }
                }));
            }, finalListener::onFailure);
        }

        beforeTransformRequest();
        // We directly modify the original search request since this is what we expect the search request processor to
        // do. Here is no need to do a stream copy to ensure the original search request is not changed.
        currentListener.onResponse(pipelinedRequest);
    }

    private ActionListener<SearchRequest> getTerminalSearchRequestActionListener(ActionListener<SearchRequest> requestListener) {
        final long pipelineStart = relativeTimeSupplier.getAsLong();

        return ActionListener.wrap(r -> {
            long took = relativeTimeSupplier.getAsLong() - pipelineStart;
            afterTransformRequest(took);
            requestListener.onResponse(r);
        }, e -> {
            long took = relativeTimeSupplier.getAsLong() - pipelineStart;
            afterTransformRequest(took);
            onTransformRequestFailure();
            requestListener.onFailure(new SearchPipelineProcessingException(e));
        });
    }

    ActionListener<SearchResponse> transformResponseListener(
        PipelinedRequest pipelinedRequest,
        ActionListener<SearchResponse> responseListener
    ) {
        if (searchResponseProcessors.isEmpty()) {
            return responseListener;
        }

        PipelineProcessingContext requestContext = pipelinedRequest.getPipelineProcessingContext();
        long[] pipelineStart = new long[1];

        final ActionListener<SearchResponse> originalListener = responseListener;
        responseListener = ActionListener.wrap(r -> {
            long took = relativeTimeSupplier.getAsLong() - pipelineStart[0];
            afterTransformResponse(took);
            originalListener.onResponse(r);
        }, e -> {
            long took = relativeTimeSupplier.getAsLong() - pipelineStart[0];
            afterTransformResponse(took);
            onTransformResponseFailure();
            originalListener.onFailure(e);
        });
        ActionListener<SearchResponse> finalListener = responseListener; // Jump directly to this one on exception.

        for (int i = searchResponseProcessors.size() - 1; i >= 0; i--) {
            final ActionListener<SearchResponse> currentFinalListener = responseListener;
            final SearchResponseProcessor processor = pipelinedRequest.source().verbosePipeline()
                ? new TrackingSearchResponseProcessorWrapper(searchResponseProcessors.get(i))
                : searchResponseProcessors.get(i);
            responseListener = ActionListener.wrap(r -> {
                beforeResponseProcessor(processor);
                final long start = relativeTimeSupplier.getAsLong();
                processor.processResponseAsync(pipelinedRequest, r, requestContext, ActionListener.wrap(rr -> {
                    long took = relativeTimeSupplier.getAsLong() - start;
                    afterResponseProcessor(processor, took);
                    currentFinalListener.onResponse(rr);
                }, e -> {
                    onResponseProcessorFailed(processor);
                    long took = relativeTimeSupplier.getAsLong() - start;
                    afterResponseProcessor(processor, took);
                    // When verbosePipeline is enabled, all processor failures are ignored to ensure the execution chain continues without
                    // interruption.TrackingSearchResponseProcessorWrapper will log all errors in detail for debugging purposes
                    if (processor.isIgnoreFailure() || pipelinedRequest.source().verbosePipeline()) {
                        logger.warn(
                            "The exception from response processor ["
                                + processor.getType()
                                + "] in the search pipeline ["
                                + id
                                + "] was ignored",
                            e
                        );
                        // Pass the previous response through to the next processor in the chain
                        currentFinalListener.onResponse(r);
                    } else {
                        currentFinalListener.onFailure(new SearchPipelineProcessingException(e));
                    }
                }));
            }, finalListener::onFailure);
        }
        final ActionListener<SearchResponse> chainListener = responseListener;
        return ActionListener.wrap(r -> {
            beforeTransformResponse();
            pipelineStart[0] = relativeTimeSupplier.getAsLong();
            chainListener.onResponse(r);
        }, originalListener::onFailure);

    }

    <Result extends SearchPhaseResult> void runSearchPhaseResultsTransformer(
        SearchPhaseResults<Result> searchPhaseResult,
        SearchPhaseContext context,
        String currentPhase,
        String nextPhase,
        PipelineProcessingContext requestContext
    ) throws SearchPipelineProcessingException {
        try {
            for (SearchPhaseResultsProcessor searchPhaseResultsProcessor : searchPhaseResultsProcessors) {
                if (currentPhase.equals(searchPhaseResultsProcessor.getBeforePhase().getName())
                    && nextPhase.equals(searchPhaseResultsProcessor.getAfterPhase().getName())) {
                    try {
                        searchPhaseResultsProcessor.process(searchPhaseResult, context, requestContext);
                    } catch (Exception e) {
                        if (searchPhaseResultsProcessor.isIgnoreFailure()) {
                            logger.warn(
                                "The exception from search phase results processor ["
                                    + searchPhaseResultsProcessor.getType()
                                    + "] in the search pipeline ["
                                    + id
                                    + "] was ignored",
                                e
                            );
                        } else {
                            throw e;
                        }
                    }

                }
            }
        } catch (RuntimeException e) {
            throw new SearchPipelineProcessingException(e);
        }
    }

    static final Pipeline NO_OP_PIPELINE = new Pipeline(
        SearchPipelineService.NOOP_PIPELINE_ID,
        "Pipeline that does not transform anything",
        0,
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList(),
        null,
        () -> 0L
    );

    /**
     * @return If this is a no-op pipeline
     */
    public boolean isNoOp() {
        return searchRequestProcessors.isEmpty() && searchPhaseResultsProcessors.isEmpty() && searchResponseProcessors.isEmpty();
    }
}
