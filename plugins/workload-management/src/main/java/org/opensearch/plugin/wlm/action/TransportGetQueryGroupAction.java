/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.service.QueryGroupPersistenceService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action for get QueryGroup
 *
 * @opensearch.experimental
 */
public class TransportGetQueryGroupAction extends HandledTransportAction<GetQueryGroupRequest, GetQueryGroupResponse> {

    private final ThreadPool threadPool;
    private final QueryGroupPersistenceService queryGroupPersistenceService;

    /**
     * Constructor for TransportGetQueryGroupAction
     *
     * @param actionName - action name
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param threadPool - a {@link ThreadPool} object
     * @param queryGroupPersistenceService - a {@link QueryGroupPersistenceService} object
     */
    @Inject
    public TransportGetQueryGroupAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        QueryGroupPersistenceService queryGroupPersistenceService
    ) {
        super(GetQueryGroupAction.NAME, transportService, actionFilters, GetQueryGroupRequest::new);
        this.threadPool = threadPool;
        this.queryGroupPersistenceService = queryGroupPersistenceService;
    }

    @Override
    protected void doExecute(Task task, GetQueryGroupRequest request, ActionListener<GetQueryGroupResponse> listener) {
        String name = request.getName();
        threadPool.executor(ThreadPool.Names.GENERIC)
            .execute(() -> queryGroupPersistenceService.getFromClusterStateMetadata(name, listener));
    }
}
