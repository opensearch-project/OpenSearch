/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.admin.cluster.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.service.PendingClusterTask;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for getting pending cluster tasks
 *
 * @opensearch.internal
 */
public class TransportPendingClusterTasksAction extends TransportClusterManagerNodeReadAction<
    PendingClusterTasksRequest,
    PendingClusterTasksResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPendingClusterTasksAction.class);

    private final ClusterService clusterService;

    @Inject
    public TransportPendingClusterTasksAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            PendingClusterTasksAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PendingClusterTasksRequest::new,
            indexNameExpressionResolver
        );
        this.clusterService = clusterService;
    }

    @Override
    protected String executor() {
        // very lightweight operation in memory, no need to fork to a thread
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PendingClusterTasksResponse read(StreamInput in) throws IOException {
        return new PendingClusterTasksResponse(in);
    }

    @Override
    protected ClusterBlockException checkBlock(PendingClusterTasksRequest request, ClusterState state) {
        return null;
    }

    @Override
    protected void clusterManagerOperation(
        PendingClusterTasksRequest request,
        ClusterState state,
        ActionListener<PendingClusterTasksResponse> listener
    ) {
        logger.trace("fetching pending tasks from cluster service");
        final List<PendingClusterTask> pendingTasks = clusterService.getClusterManagerService().pendingTasks();
        logger.trace("done fetching pending tasks from cluster service");
        listener.onResponse(new PendingClusterTasksResponse(pendingTasks));
    }
}
