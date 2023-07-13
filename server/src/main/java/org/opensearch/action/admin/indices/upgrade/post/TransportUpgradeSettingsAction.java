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

package org.opensearch.action.admin.indices.upgrade.post;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MetadataUpdateSettingsService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Transport action for upgrading index settings
 *
 * @opensearch.internal
 */
public class TransportUpgradeSettingsAction extends TransportClusterManagerNodeAction<UpgradeSettingsRequest, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportUpgradeSettingsAction.class);

    private final MetadataUpdateSettingsService updateSettingsService;

    @Inject
    public TransportUpgradeSettingsAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        MetadataUpdateSettingsService updateSettingsService,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ActionFilters actionFilters
    ) {
        super(
            UpgradeSettingsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            UpgradeSettingsRequest::new,
            indexNameExpressionResolver
        );
        this.updateSettingsService = updateSettingsService;
    }

    @Override
    protected String executor() {
        // we go async right away....
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(UpgradeSettingsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        final UpgradeSettingsRequest request,
        final ClusterState state,
        final ActionListener<AcknowledgedResponse> listener
    ) {
        UpgradeSettingsClusterStateUpdateRequest clusterStateUpdateRequest = new UpgradeSettingsClusterStateUpdateRequest().ackTimeout(
            request.timeout()
        ).versions(request.versions()).masterNodeTimeout(request.clusterManagerNodeTimeout());

        updateSettingsService.upgradeIndexSettings(clusterStateUpdateRequest, new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse response) {
                listener.onResponse(new AcknowledgedResponse(response.isAcknowledged()));
            }

            @Override
            public void onFailure(Exception t) {
                logger.debug(
                    () -> new ParameterizedMessage(
                        "failed to upgrade minimum compatibility version settings on indices [{}]",
                        request.versions().keySet()
                    ),
                    t
                );
                listener.onFailure(t);
            }
        });
    }
}
