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
 * Transport action for update Resource Limit Group
 *
 * @opensearch.internal
 */
public class TransportUpdateResourceLimitGroupAction extends HandledTransportAction<
    UpdateResourceLimitGroupRequest,
    UpdateResourceLimitGroupResponse> {

    private final ThreadPool threadPool;
    private final Persistable<ResourceLimitGroup> resourceLimitGroupPersistenceService;

    /**
     * Constructor for TransportUpdateResourceLimitGroupAction
     *
     * @param actionName - acrtion name
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param threadPool - a {@link ThreadPool} object
     * @param resourceLimitGroupPersistenceService - a {@link Persistable} object
     */
    @Inject
    public TransportUpdateResourceLimitGroupAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        Persistable<ResourceLimitGroup> resourceLimitGroupPersistenceService
    ) {
        super(UpdateResourceLimitGroupAction.NAME, transportService, actionFilters, UpdateResourceLimitGroupRequest::new);
        this.threadPool = threadPool;
        this.resourceLimitGroupPersistenceService = resourceLimitGroupPersistenceService;
    }

    @Override
    protected void doExecute(
        Task task,
        UpdateResourceLimitGroupRequest request,
        ActionListener<UpdateResourceLimitGroupResponse> listener
    ) {
        ResourceLimitGroup resourceLimitGroup = new ResourceLimitGroup(
            request.getName(),
            null,
            request.getResourceLimits(),
            request.getEnforcement(),
            null,
            request.getUpdatedAt()
        );
        threadPool.executor(ThreadPool.Names.GENERIC)
            .execute(() -> resourceLimitGroupPersistenceService.update(resourceLimitGroup, listener));
    }
}
