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

package org.opensearch.action.admin.indices.close;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.DestructiveOperations;
import org.opensearch.action.support.TransportIndicesResolvingAction;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataIndexStateService;
import org.opensearch.cluster.metadata.ResolvedIndices;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.node.remotestore.RemoteStoreNodeService.CompatibilityMode;
import org.opensearch.node.remotestore.RemoteStoreNodeService.Direction;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Collections;

/**
 * Close index action
 *
 * @opensearch.internal
 */
public class TransportCloseIndexAction extends TransportClusterManagerNodeAction<CloseIndexRequest, CloseIndexResponse>
    implements
        TransportIndicesResolvingAction<CloseIndexRequest> {

    private static final Logger logger = LogManager.getLogger(TransportCloseIndexAction.class);

    private final MetadataIndexStateService indexStateService;
    private final DestructiveOperations destructiveOperations;
    private volatile boolean closeIndexEnabled;
    public static final Setting<Boolean> CLUSTER_INDICES_CLOSE_ENABLE_SETTING = Setting.boolSetting(
        "cluster.indices.close.enable",
        true,
        Property.Dynamic,
        Property.NodeScope
    );

    @Inject
    public TransportCloseIndexAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataIndexStateService indexStateService,
        ClusterSettings clusterSettings,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        DestructiveOperations destructiveOperations
    ) {
        super(
            CloseIndexAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            CloseIndexRequest::new,
            indexNameExpressionResolver
        );
        this.indexStateService = indexStateService;
        this.destructiveOperations = destructiveOperations;
        this.closeIndexEnabled = CLUSTER_INDICES_CLOSE_ENABLE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_INDICES_CLOSE_ENABLE_SETTING, this::setCloseIndexEnabled);
    }

    private void setCloseIndexEnabled(boolean closeIndexEnabled) {
        this.closeIndexEnabled = closeIndexEnabled;
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CloseIndexResponse read(StreamInput in) throws IOException {
        return new CloseIndexResponse(in);
    }

    @Override
    protected void doExecute(Task task, CloseIndexRequest request, ActionListener<CloseIndexResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        if (closeIndexEnabled == false) {
            throw new IllegalStateException(
                "closing indices is disabled - set ["
                    + CLUSTER_INDICES_CLOSE_ENABLE_SETTING.getKey()
                    + ": true] to enable it. NOTE: closed indices still consume a significant amount of diskspace"
            );
        }
        validateRemoteMigration();
        super.doExecute(task, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(CloseIndexRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void clusterManagerOperation(
        final CloseIndexRequest request,
        final ClusterState state,
        final ActionListener<CloseIndexResponse> listener
    ) {
        throw new UnsupportedOperationException("The task parameter is required");
    }

    @Override
    protected void clusterManagerOperation(
        final Task task,
        final CloseIndexRequest request,
        final ClusterState state,
        final ActionListener<CloseIndexResponse> listener
    ) throws Exception {
        final Index[] concreteIndices = resolveIndices(state, request).concreteIndicesAsArray();
        if (concreteIndices == null || concreteIndices.length == 0) {
            listener.onResponse(new CloseIndexResponse(true, false, Collections.emptyList()));
            return;
        }

        final CloseIndexClusterStateUpdateRequest closeRequest = new CloseIndexClusterStateUpdateRequest(task.getId()).ackTimeout(
            request.timeout()
        )
            .clusterManagerNodeTimeout(request.clusterManagerNodeTimeout())
            .waitForActiveShards(request.waitForActiveShards())
            .indices(concreteIndices);
        indexStateService.closeIndices(closeRequest, ActionListener.delegateResponse(listener, (delegatedListener, t) -> {
            logger.debug(() -> new ParameterizedMessage("failed to close indices [{}]", (Object) concreteIndices), t);
            delegatedListener.onFailure(t);
        }));
    }

    private ResolvedIndices.Local.Concrete resolveIndices(ClusterState state, CloseIndexRequest request) {
        return indexNameExpressionResolver.concreteResolvedIndices(state, request);
    }

    @Override
    public ResolvedIndices resolveIndices(CloseIndexRequest request) {
        return ResolvedIndices.of(resolveIndices(clusterService.state(), request));
    }

    /**
     * Reject close index request if cluster mode is [MIXED] and migration direction is [RemoteStore]
     * @throws IllegalStateException if cluster mode is [MIXED] and migration direction is [RemoteStore]
     */
    private void validateRemoteMigration() {
        ClusterSettings clusterSettings = clusterService.getClusterSettings();
        CompatibilityMode compatibilityMode = clusterSettings.get(RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING);
        Direction migrationDirection = clusterSettings.get(RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING);
        if (compatibilityMode == CompatibilityMode.MIXED && migrationDirection == Direction.REMOTE_STORE) {
            throw new IllegalStateException("Cannot close index while remote migration is ongoing");
        }
    }
}
