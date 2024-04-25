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
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.SearchPhaseResult;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
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

    void transformRequest(SearchRequest request, ActionListener<SearchRequest> requestListener, PipelineProcessingContext requestContext)
        throws SearchPipelineProcessingException {
        if (searchRequestProcessors.isEmpty()) {
            requestListener.onResponse(request);
            return;
        }

        try (BytesStreamOutput bytesStreamOutput = new BytesStreamOutput()) {
            request.writeTo(bytesStreamOutput);
            try (StreamInput in = bytesStreamOutput.bytes().streamInput()) {
                try (StreamInput input = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry)) {
                    request = new SearchRequest(input);
                }
            }
        } catch (IOException e) {
            requestListener.onFailure(new SearchPipelineProcessingException(e));
            return;
        }

        ActionListener<SearchRequest> finalListener = getTerminalSearchRequestActionListener(requestListener, requestContext);

        // Chain listeners back-to-front
        ActionListener<SearchRequest> currentListener = finalListener;
        for (int i = searchRequestProcessors.size() - 1; i >= 0; i--) {
            final ActionListener<SearchRequest> nextListener = currentListener;
            SearchRequestProcessor processor = searchRequestProcessors.get(i);
            currentListener = ActionListener.wrap(r -> {
                long start = relativeTimeSupplier.getAsLong();
                beforeRequestProcessor(processor);
                processor.processRequestAsync(r, requestContext, ActionListener.wrap(rr -> {
                    long took = TimeUnit.NANOSECONDS.toMillis(relativeTimeSupplier.getAsLong() - start);
                    afterRequestProcessor(processor, took);
                    nextListener.onResponse(rr);
                }, e -> {
                    long took = TimeUnit.NANOSECONDS.toMillis(relativeTimeSupplier.getAsLong() - start);
                    afterRequestProcessor(processor, took);
                    onRequestProcessorFailed(processor);
                    if (processor.isIgnoreFailure()) {
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
        currentListener.onResponse(request);
    }

    private ActionListener<SearchRequest> getTerminalSearchRequestActionListener(
        ActionListener<SearchRequest> requestListener,
        PipelineProcessingContext requestContext
    ) {
        final long pipelineStart = relativeTimeSupplier.getAsLong();

        return ActionListener.wrap(r -> {
            long took = TimeUnit.NANOSECONDS.toMillis(relativeTimeSupplier.getAsLong() - pipelineStart);
            afterTransformRequest(took);
            requestListener.onResponse(new PipelinedRequest(this, r, requestContext));
        }, e -> {
            long took = TimeUnit.NANOSECONDS.toMillis(relativeTimeSupplier.getAsLong() - pipelineStart);
            afterTransformRequest(took);
            onTransformRequestFailure();
            requestListener.onFailure(new SearchPipelineProcessingException(e));
        });
    }

    ActionListener<SearchResponse> transformResponseListener(
        SearchRequest request,
        ActionListener<SearchResponse> responseListener,
        PipelineProcessingContext requestContext
    ) {
        if (searchResponseProcessors.isEmpty()) {
            // No response transformation necessary
            return responseListener;
        }

        long[] pipelineStart = new long[1];

        final ActionListener<SearchResponse> originalListener = responseListener;
        responseListener = ActionListener.wrap(r -> {
            long took = TimeUnit.NANOSECONDS.toMillis(relativeTimeSupplier.getAsLong() - pipelineStart[0]);
            afterTransformResponse(took);
            originalListener.onResponse(r);
        }, e -> {
            long took = TimeUnit.NANOSECONDS.toMillis(relativeTimeSupplier.getAsLong() - pipelineStart[0]);
            afterTransformResponse(took);
            onTransformResponseFailure();
            originalListener.onFailure(e);
        });
        ActionListener<SearchResponse> finalListener = responseListener; // Jump directly to this one on exception.

        for (int i = searchResponseProcessors.size() - 1; i >= 0; i--) {
            final ActionListener<SearchResponse> currentFinalListener = responseListener;
            final SearchResponseProcessor processor = searchResponseProcessors.get(i);

            responseListener = ActionListener.wrap(r -> {
                beforeResponseProcessor(processor);
                final long start = relativeTimeSupplier.getAsLong();
                processor.processResponseAsync(request, r, requestContext, ActionListener.wrap(rr -> {
                    long took = TimeUnit.NANOSECONDS.toMillis(relativeTimeSupplier.getAsLong() - start);
                    afterResponseProcessor(processor, took);
                    currentFinalListener.onResponse(rr);
                }, e -> {
                    onResponseProcessorFailed(processor);
                    long took = TimeUnit.NANOSECONDS.toMillis(relativeTimeSupplier.getAsLong() - start);
                    afterResponseProcessor(processor, took);
                    if (processor.isIgnoreFailure()) {
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
}
