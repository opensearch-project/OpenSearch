/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.metadata.ResourceLimitGroup;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.resource_limit_group.service.Persistable;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

/**
 * Transport action for get Resource Limit Group
 *
 * @opensearch.internal
 */
public class TransportGetResourceLimitGroupAction extends HandledTransportAction<
    GetResourceLimitGroupRequest,
    GetResourceLimitGroupResponse> {

    private final ThreadPool threadPool;
    private final Persistable<ResourceLimitGroup> resourceLimitGroupPersistenceService;

    /**
     * Constructor for TransportGetResourceLimitGroupAction
     *
     * @param actionName - acrtion name
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param threadPool - a {@link ThreadPool} object
     * @param resourceLimitGroupPersistenceService - a {@link Persistable} object
     */
    @Inject
    public TransportGetResourceLimitGroupAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Persistable<ResourceLimitGroup> resourceLimitGroupPersistenceService
    ) {
        super(GetResourceLimitGroupAction.NAME, transportService, actionFilters, GetResourceLimitGroupRequest::new);
        this.threadPool = threadPool;
        this.resourceLimitGroupPersistenceService = resourceLimitGroupPersistenceService;
    }

    @Override
    protected void doExecute(Task task, GetResourceLimitGroupRequest request, ActionListener<GetResourceLimitGroupResponse> listener) {
        String name = request.getName();
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> resourceLimitGroupPersistenceService.get(name, listener));
    }
}
