/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.Nullable;
import org.opensearch.index.IndexSettings;
import org.opensearch.ingest.IngestService;
import org.opensearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles ingest pipeline resolution and execution for pull-based ingestion.
 *
 * <p>Resolves configured pipelines from index settings at initialization and executes them
 * synchronously by bridging IngestService's async callback API with CompletableFuture.
 * Also registers a dynamic settings listener to pick up runtime changes to {@code final_pipeline}.
 * Only {@code final_pipeline} is supported.
 *
 * <p>Unlike push-based indexing, pipeline execution in pull-based ingestion does not require the
 * node to have the {@code ingest} role. Transformations are executed locally on the node hosting the
 * shard, and requests are not forwarded to dedicated ingest nodes.
 */
public class IngestPipelineExecutor {

    private static final Logger logger = LogManager.getLogger(IngestPipelineExecutor.class);

    // TODO: consider making this configurable via index settings if use cases with slow processors arise
    static final long PIPELINE_EXECUTION_TIMEOUT_SECONDS = 30;

    // TODO: explore synchronous pipeline execution (IngestService.executeBulkRequestSync) to avoid
    // thread pool dispatch and execute pipelines directly on the processor thread

    private final IngestService ingestService;
    private final String index;
    private volatile String resolvedFinalPipeline;

    /**
     * Creates an IngestPipelineExecutor for the given index.
     * Resolves the final pipeline from index settings and registers a dynamic settings listener.
     *
     * @param ingestService the ingest service for pipeline execution
     * @param index the index name
     * @param indexSettings the index settings to resolve a pipeline from and register listener on
     */
    public IngestPipelineExecutor(IngestService ingestService, String index, IndexSettings indexSettings) {
        this.ingestService = Objects.requireNonNull(ingestService);
        this.index = Objects.requireNonNull(index);
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(IndexSettings.FINAL_PIPELINE, this::updateFinalPipeline);
        updateFinalPipeline(IndexSettings.FINAL_PIPELINE.get(indexSettings.getSettings()));
    }

    /**
     * Visible for testing. Creates an executor with a pre-resolved pipeline name,
     * bypassing resolution from index settings.
     *
     * @param ingestService the ingest service
     * @param index the index name
     * @param finalPipeline the resolved final pipeline name, or null if no pipeline is configured
     */
    IngestPipelineExecutor(IngestService ingestService, String index, @Nullable String finalPipeline) {
        this.ingestService = Objects.requireNonNull(ingestService);
        this.index = Objects.requireNonNull(index);
        this.resolvedFinalPipeline = finalPipeline;
    }

    /**
     * Updates the cached final pipeline name. Called on initial resolution and on dynamic settings change.
     */
    void updateFinalPipeline(String finalPipeline) {
        if (IngestService.NOOP_PIPELINE_NAME.equals(finalPipeline)) {
            resolvedFinalPipeline = null;
        } else {
            resolvedFinalPipeline = finalPipeline;
        }
    }

    /**
     * Executes final_pipeline on the source map synchronously using CompletableFuture to bridge
     * IngestService's async callback API.
     *
     * @param id document ID
     * @param sourceMap source map to transform
     * @return the transformed source map, or null if the document was dropped by the pipeline
     * @throws Exception if pipeline execution fails
     */
    public Map<String, Object> executePipelines(String id, Map<String, Object> sourceMap) throws Exception {
        final String finalPipeline = resolvedFinalPipeline;
        if (finalPipeline == null) {
            return sourceMap;
        }

        // Build IndexRequest to carry the document through the pipeline
        IndexRequest indexRequest = new IndexRequest(index);
        indexRequest.id(id);
        indexRequest.source(sourceMap);

        indexRequest.setPipeline(IngestService.NOOP_PIPELINE_NAME);
        indexRequest.setFinalPipeline(finalPipeline);
        indexRequest.isPipelineResolved(true);

        final String originalId = id;
        final String originalRouting = indexRequest.routing();

        CompletableFuture<Void> future = new CompletableFuture<>();
        AtomicBoolean dropped = new AtomicBoolean(false);

        ingestService.executeBulkRequest(
            1,
            Collections.singletonList(indexRequest),
            (slot, e) -> future.completeExceptionally(e),
            (thread, e) -> {
                if (e != null) {
                    future.completeExceptionally(e);
                } else {
                    future.complete(null);
                }
            },
            slot -> dropped.set(true),
            ThreadPool.Names.WRITE
        );

        // Block until pipeline execution completes (with timeout)
        try {
            future.get(PIPELINE_EXECUTION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new RuntimeException("Ingest pipeline execution timed out after [" + PIPELINE_EXECUTION_TIMEOUT_SECONDS + "] seconds", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Ingest pipeline execution was interrupted", e);
        } catch (ExecutionException e) {
            throw new RuntimeException("Ingest pipeline execution failed", e.getCause());
        }

        if (dropped.get()) {
            return null;
        }

        // verify _id and _routing have not been mutated
        if (Objects.equals(originalId, indexRequest.id()) == false) {
            throw new IllegalStateException(
                "Ingest pipeline attempted to change _id from ["
                    + originalId
                    + "] to ["
                    + indexRequest.id()
                    + "]. _id mutations are not allowed in pull-based ingestion."
            );
        }
        if (Objects.equals(originalRouting, indexRequest.routing()) == false) {
            throw new IllegalStateException(
                "Ingest pipeline attempted to change _routing. _routing mutations are not allowed in pull-based ingestion."
            );
        }

        // _index change is already blocked by final_pipeline semantics in IngestService

        return indexRequest.sourceAsMap();
    }
}
