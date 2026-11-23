/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.tasks.TaskResourceTrackingService;

/**
 * SearchTaskRequestOperationsListener subscriber for operations on search tasks resource usages.
 * Listener ensures to refreshResourceStats on request end capturing the search task resource usage
 * upon request completion.
 *
 */
public final class SearchTaskRequestOperationsListener extends SearchRequestOperationsListener {
    private final TaskResourceTrackingService taskResourceTrackingService;

    public SearchTaskRequestOperationsListener(TaskResourceTrackingService taskResourceTrackingService) {
        this.taskResourceTrackingService = taskResourceTrackingService;
    }

    @Override
    public void onRequestEnd(SearchRequestContext searchRequestContext) {
        // Without a search on this node, there is no task to refresh and no shard usage to remove.
        final SearchPhaseContext context = searchRequestContext.getPhaseContext();
        if (context == null) {
            return;
        }
        // Refresh the coordinator node level resource usages
        taskResourceTrackingService.refreshResourceStats(context.getTask());
        // Remove the shard level resource usages from thread context
        taskResourceTrackingService.removeTaskResourceUsage();
    }

    @Override
    public void onRequestFailure(SearchRequestContext searchRequestContext) {
        if (searchRequestContext.getPhaseContext() == null) {
            return;
        }
        taskResourceTrackingService.removeTaskResourceUsage();
    }
}
