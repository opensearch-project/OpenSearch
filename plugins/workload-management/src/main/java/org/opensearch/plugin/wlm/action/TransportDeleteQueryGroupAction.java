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
import org.opensearch.cluster.metadata.QueryGroup;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.action.service.Persistable;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action for delete QueryGroup
 *
 * @opensearch.internal
 */
public class TransportDeleteQueryGroupAction extends HandledTransportAction<DeleteQueryGroupRequest, DeleteQueryGroupResponse> {

    private final ThreadPool threadPool;
    private final Persistable<QueryGroup> queryGroupPersistenceService;

    /**
     * Constructor for TransportDeleteQueryGroupAction
     *
     * @param actionName - action name
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param threadPool - a {@link ThreadPool} object
     * @param queryGroupPersistenceService - a {@link Persistable} object
     */
    @Inject
    public TransportDeleteQueryGroupAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Persistable<QueryGroup> queryGroupPersistenceService
    ) {
        super(DeleteQueryGroupAction.NAME, transportService, actionFilters, DeleteQueryGroupRequest::new);
        this.threadPool = threadPool;
        this.queryGroupPersistenceService = queryGroupPersistenceService;
    }

    @Override
    protected void doExecute(Task task, DeleteQueryGroupRequest request, ActionListener<DeleteQueryGroupResponse> listener) {
        String name = request.getName();
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> queryGroupPersistenceService.delete(name, listener));
    }
}
