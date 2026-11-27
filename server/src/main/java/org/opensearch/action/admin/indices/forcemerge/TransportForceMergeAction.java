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

package org.opensearch.action.admin.indices.forcemerge;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.indices.IndicesService;
import org.opensearch.ratelimitting.admissioncontrol.AdmissionControlService;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * ForceMerge index/indices action.
 *
 * @opensearch.internal
 */
public class TransportForceMergeAction extends TransportBroadcastByNodeAction<
    ForceMergeRequest,
    ForceMergeResponse,
    TransportBroadcastByNodeAction.EmptyResult> {

    /**
     * Gates admission control for the force merge API independently of the other action types. The controllers' own
     * mode settings still apply, so a force merge is only ever rejected when this is {@code true} <em>and</em> a
     * controller is enforced. Enabled by default, so force merges follow the controllers' modes like the other action
     * types; set it to {@code false} to exempt force merges without changing admission control for search, indexing or
     * cluster admin actions. Dynamic: a change is picked up by the next shard to start merging, including later shards
     * of a request that is already in progress.
     */
    public static final Setting<Boolean> FORCE_MERGE_ADMISSION_CONTROL_ENABLED = Setting.boolSetting(
        "admission_control.force_merge.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private final IndicesService indicesService;
    private final AdmissionControlService admissionControlService;
    private volatile boolean admissionControlEnabled;

    @Inject
    public TransportForceMergeAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        AdmissionControlService admissionControlService
    ) {
        super(
            ForceMergeAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            ForceMergeRequest::new,
            ThreadPool.Names.FORCE_MERGE
        );
        this.indicesService = indicesService;
        this.admissionControlService = admissionControlService;
        this.admissionControlEnabled = clusterService.getClusterSettings().get(FORCE_MERGE_ADMISSION_CONTROL_ENABLED);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(FORCE_MERGE_ADMISSION_CONTROL_ENABLED, enabled -> this.admissionControlEnabled = enabled);
    }

    @Override
    protected EmptyResult readShardResult(StreamInput in) throws IOException {
        return EmptyResult.readEmptyResultFrom(in);
    }

    @Override
    protected ForceMergeResponse newResponse(
        ForceMergeRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<EmptyResult> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new ForceMergeResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ForceMergeRequest readRequestFrom(StreamInput in) throws IOException {
        return new ForceMergeRequest(in);
    }

    /**
     * Runs on the data node, on the {@link ThreadPool.Names#FORCE_MERGE} pool. Admission control is evaluated per shard,
     * immediately before the merge starts, so that the decision reflects the resource usage of the node actually doing
     * the work at the time the work begins. Shards of one node request are merged sequentially and each merge can run
     * for a long time, so a single node-level check would go stale for the later shards.
     * A rejection surfaces as an {@link org.opensearch.core.concurrency.OpenSearchRejectedExecutionException} shard
     * failure; the remaining shards are still attempted.
     */
    @Override
    protected EmptyResult shardOperation(ForceMergeRequest request, ShardRouting shardRouting) throws IOException {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRouting.shardId().getIndex()).getShard(shardRouting.shardId().id());
        if (isAdmissionControlRequired(indexShard)) {
            admissionControlService.applyTransportAdmissionControl(actionName, AdmissionControlActionType.FORCE_MERGE);
        }
        indexShard.forceMerge(request);
        return EmptyResult.INSTANCE;
    }

    /**
     * Admission control applies only when {@link #FORCE_MERGE_ADMISSION_CONTROL_ENABLED} is set; when it is not, the
     * controllers are not consulted at all, so no rejection is recorded even in monitor mode.
     * <p>
     * Only copies that can actually merge are admission controlled. A copy that is not {@link IndexShardState#STARTED}
     * (for example one that is still recovering, or the target of a relocation) is left to {@link IndexShard#forceMerge},
     * which rejects it with an {@link org.opensearch.index.shard.IllegalIndexShardStateException}. That exception is
     * treated as "shard not available" and left out of the response's failures, exactly as before admission control
     * was added; checking admission first would instead charge a rejection and report a spurious 429 for it. The state
     * may change right after this check, which is harmless because {@link IndexShard#forceMerge} checks it again.
     * <p>
     * Replicas of segment replication or remote store backed indices do not merge locally: their engine's force merge
     * is a no-op and the merged segments are copied from the primary. Admission control is skipped for them because
     * there is no local merge to protect, and rejecting them would only report spurious shard failures. The primary
     * flag is read from the shard's current routing entry rather than the coordinator's routing, so a replica that was
     * promoted after the request was built (and therefore merges) is admission controlled.
     */
    boolean isAdmissionControlRequired(IndexShard indexShard) {
        if (admissionControlEnabled == false || indexShard.state() != IndexShardState.STARTED) {
            return false;
        }
        return indexShard.routingEntry().primary() || indexShard.indexSettings().isSegRepEnabledOrRemoteNode() == false;
    }

    /**
     * The force merge request works against *all* shards by default, but it can work against all primary shards only
     * by setting primary_only to true.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, ForceMergeRequest request, String[] concreteIndices) {
        if (request.primaryOnly()) {
            return clusterState.routingTable().allShardsSatisfyingPredicate(concreteIndices, ShardRouting::primary);
        } else {
            return clusterState.routingTable().allShards(concreteIndices);
        }
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ForceMergeRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ForceMergeRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }
}
