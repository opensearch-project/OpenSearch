/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.search.SearchService;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.StreamTransportService;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Transport search action for streaming search
 * @opensearch.internal
 */
public class StreamTransportSearchAction extends TransportSearchAction {
    @Inject
    public StreamTransportSearchAction(
        NodeClient client,
        ThreadPool threadPool,
        CircuitBreakerService circuitBreakerService,
        @Nullable StreamTransportService transportService,
        SearchService searchService,
        @Nullable StreamSearchTransportService searchTransportService,
        SearchPhaseController searchPhaseController,
        ClusterService clusterService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NamedWriteableRegistry namedWriteableRegistry,
        SearchPipelineService searchPipelineService,
        MetricsRegistry metricsRegistry,
        SearchRequestOperationsCompositeListenerFactory searchRequestOperationsCompositeListenerFactory,
        Tracer tracer,
        TaskResourceTrackingService taskResourceTrackingService
    ) {
        super(
            client,
            threadPool,
            circuitBreakerService,
            transportService,
            searchService,
            searchTransportService,
            searchPhaseController,
            clusterService,
            actionFilters,
            indexNameExpressionResolver,
            namedWriteableRegistry,
            searchPipelineService,
            metricsRegistry,
            searchRequestOperationsCompositeListenerFactory,
            tracer,
            taskResourceTrackingService
        );
    }
}
