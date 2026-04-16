/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.exec.action.FragmentExecutionResponse;
import org.opensearch.analytics.exec.AnalyticsSearchService;
import org.opensearch.analytics.exec.task.AnalyticsShardTask;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

/**
 * Transport action registered on data nodes that resolves an {@link IndexShard}
 * from {@link IndicesService} and delegates to {@link AnalyticsSearchService}
 * for shard-level fragment execution.
 *
 * <p>This is the only component that holds {@code IndicesService} —
 * {@code AnalyticsSearchService} receives an already-resolved shard.
 */
public class TransportAnalyticsShardAction extends HandledTransportAction<FragmentExecutionRequest, FragmentExecutionResponse> {

    public static final String ACTION_NAME = "indices:data/read/analytics/shard";

    private final IndicesService indicesService;
    private final AnalyticsSearchService searchService;

    @Inject
    public TransportAnalyticsShardAction(
        TransportService transportService,
        ActionFilters actionFilters,
        IndicesService indicesService,
        AnalyticsSearchService searchService
    ) {
        super(ACTION_NAME, transportService, actionFilters, FragmentExecutionRequest::new);
        this.indicesService = indicesService;
        this.searchService = searchService;
    }

    @Override
    protected void doExecute(Task task, FragmentExecutionRequest request, ActionListener<FragmentExecutionResponse> listener) {
        try {
            AnalyticsShardTask shardTask = task instanceof AnalyticsShardTask ? (AnalyticsShardTask) task : null;
            IndexShard shard = indicesService.indexServiceSafe(request.getShardId().getIndex()).getShard(request.getShardId().id());
            listener.onResponse(searchService.executeFragment(request, shard, shardTask));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
