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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch;

import org.opensearch.action.support.replication.ReplicationOperation;
import org.opensearch.cluster.action.shard.ShardStateAction;
import org.opensearch.cluster.routing.NodeWeighedAwayException;
import org.opensearch.cluster.routing.PreferenceBasedSearchNotAllowedException;
import org.opensearch.cluster.routing.UnsupportedWeightedRoutingStateException;
import org.opensearch.cluster.service.ClusterManagerThrottlingException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.index.snapshots.IndexShardSnapshotException;
import org.opensearch.search.aggregations.MultiBucketConsumerService;
import org.opensearch.search.pipeline.SearchPipelineProcessingException;
import org.opensearch.snapshots.SnapshotInUseDeletionException;
import org.opensearch.transport.TcpTransport;

import java.io.IOException;

import static org.opensearch.BaseOpenSearchException.OpenSearchExceptionHandleRegistry.registerExceptionHandle;
import static org.opensearch.Version.V_2_1_0;
import static org.opensearch.Version.V_2_4_0;
import static org.opensearch.Version.V_2_5_0;
import static org.opensearch.Version.V_2_6_0;
import static org.opensearch.Version.V_2_7_0;
import static org.opensearch.Version.V_3_0_0;

/**
 * A base class for all opensearch exceptions.
 *
 * @opensearch.internal
 */
public class OpenSearchException extends BaseOpenSearchException {

    /**
     * Setting a higher base exception id to avoid conflicts.
     */
    private static final int CUSTOM_ELASTICSEARCH_EXCEPTIONS_BASE_ID = 10000;

    static {
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.search.dfs.DfsPhaseExecutionException.class,
                org.opensearch.search.dfs.DfsPhaseExecutionException::new,
                1,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.common.util.CancellableThreads.ExecutionCancelledException.class,
                org.opensearch.common.util.CancellableThreads.ExecutionCancelledException::new,
                2,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.discovery.ClusterManagerNotDiscoveredException.class,
                org.opensearch.discovery.ClusterManagerNotDiscoveredException::new,
                3,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.OpenSearchSecurityException.class,
                org.opensearch.OpenSearchSecurityException::new,
                4,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.snapshots.IndexShardRestoreException.class,
                org.opensearch.index.snapshots.IndexShardRestoreException::new,
                5,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.indices.IndexClosedException.class,
                org.opensearch.indices.IndexClosedException::new,
                6,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.http.BindHttpException.class,
                org.opensearch.http.BindHttpException::new,
                7,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.action.search.ReduceSearchPhaseException.class,
                org.opensearch.action.search.ReduceSearchPhaseException::new,
                8,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.node.NodeClosedException.class,
                org.opensearch.node.NodeClosedException::new,
                9,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.engine.SnapshotFailedEngineException.class,
                org.opensearch.index.engine.SnapshotFailedEngineException::new,
                10,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.shard.ShardNotFoundException.class,
                org.opensearch.index.shard.ShardNotFoundException::new,
                11,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.ConnectTransportException.class,
                org.opensearch.transport.ConnectTransportException::new,
                12,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.NotSerializableTransportException.class,
                org.opensearch.transport.NotSerializableTransportException::new,
                13,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.ResponseHandlerFailureTransportException.class,
                org.opensearch.transport.ResponseHandlerFailureTransportException::new,
                14,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.indices.IndexCreationException.class,
                org.opensearch.indices.IndexCreationException::new,
                15,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.IndexNotFoundException.class,
                org.opensearch.index.IndexNotFoundException::new,
                16,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.cluster.routing.IllegalShardRoutingStateException.class,
                org.opensearch.cluster.routing.IllegalShardRoutingStateException::new,
                17,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.action.support.broadcast.BroadcastShardOperationFailedException.class,
                org.opensearch.action.support.broadcast.BroadcastShardOperationFailedException::new,
                18,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.ResourceNotFoundException.class,
                org.opensearch.ResourceNotFoundException::new,
                19,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.ActionTransportException.class,
                org.opensearch.transport.ActionTransportException::new,
                20,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.OpenSearchGenerationException.class,
                org.opensearch.OpenSearchGenerationException::new,
                21,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 22 was CreateFailedEngineException
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.shard.IndexShardStartedException.class,
                org.opensearch.index.shard.IndexShardStartedException::new,
                23,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.search.SearchContextMissingException.class,
                org.opensearch.search.SearchContextMissingException::new,
                24,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.script.GeneralScriptException.class,
                org.opensearch.script.GeneralScriptException::new,
                25,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 26 was BatchOperationException
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.snapshots.SnapshotCreationException.class,
                org.opensearch.snapshots.SnapshotCreationException::new,
                27,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 28 was DeleteFailedEngineException, deprecated in 6.0, removed in 7.0
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.engine.DocumentMissingException.class,
                org.opensearch.index.engine.DocumentMissingException::new,
                29,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.snapshots.SnapshotException.class,
                org.opensearch.snapshots.SnapshotException::new,
                30,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.indices.InvalidAliasNameException.class,
                org.opensearch.indices.InvalidAliasNameException::new,
                31,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.indices.InvalidIndexNameException.class,
                org.opensearch.indices.InvalidIndexNameException::new,
                32,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.indices.IndexPrimaryShardNotAllocatedException.class,
                org.opensearch.indices.IndexPrimaryShardNotAllocatedException::new,
                33,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.TransportException.class,
                org.opensearch.transport.TransportException::new,
                34,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.OpenSearchParseException.class,
                org.opensearch.OpenSearchParseException::new,
                35,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.search.SearchException.class,
                org.opensearch.search.SearchException::new,
                36,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.mapper.MapperException.class,
                org.opensearch.index.mapper.MapperException::new,
                37,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.indices.InvalidTypeNameException.class,
                org.opensearch.indices.InvalidTypeNameException::new,
                38,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.snapshots.SnapshotRestoreException.class,
                org.opensearch.snapshots.SnapshotRestoreException::new,
                39,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.shard.IndexShardClosedException.class,
                org.opensearch.index.shard.IndexShardClosedException::new,
                41,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.indices.recovery.RecoverFilesRecoveryException.class,
                org.opensearch.indices.recovery.RecoverFilesRecoveryException::new,
                42,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.translog.TruncatedTranslogException.class,
                org.opensearch.index.translog.TruncatedTranslogException::new,
                43,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.indices.recovery.RecoveryFailedException.class,
                org.opensearch.indices.recovery.RecoveryFailedException::new,
                44,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.shard.IndexShardRelocatedException.class,
                org.opensearch.index.shard.IndexShardRelocatedException::new,
                45,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.NodeShouldNotConnectException.class,
                org.opensearch.transport.NodeShouldNotConnectException::new,
                46,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 47 used to be for IndexTemplateAlreadyExistsException which was deprecated in 5.1 removed in 6.0
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.translog.TranslogCorruptedException.class,
                org.opensearch.index.translog.TranslogCorruptedException::new,
                48,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.cluster.block.ClusterBlockException.class,
                org.opensearch.cluster.block.ClusterBlockException::new,
                49,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.search.fetch.FetchPhaseExecutionException.class,
                org.opensearch.search.fetch.FetchPhaseExecutionException::new,
                50,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 51 used to be for IndexShardAlreadyExistsException which was deprecated in 5.1 removed in 6.0
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.engine.VersionConflictEngineException.class,
                org.opensearch.index.engine.VersionConflictEngineException::new,
                52,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.engine.EngineException.class,
                org.opensearch.index.engine.EngineException::new,
                53,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 54 was DocumentAlreadyExistsException, which is superseded by VersionConflictEngineException
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.action.NoSuchNodeException.class,
                org.opensearch.action.NoSuchNodeException::new,
                55,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.common.settings.SettingsException.class,
                org.opensearch.common.settings.SettingsException::new,
                56,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.indices.IndexTemplateMissingException.class,
                org.opensearch.indices.IndexTemplateMissingException::new,
                57,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.SendRequestTransportException.class,
                org.opensearch.transport.SendRequestTransportException::new,
                58,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 59 used to be OpenSearchRejectedExecutionException
        // 60 used to be for EarlyTerminationException
        // 61 used to be for RoutingValidationException
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.indices.AliasFilterParsingException.class,
                org.opensearch.indices.AliasFilterParsingException::new,
                63,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 64 was DeleteByQueryFailedEngineException, which was removed in 5.0
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.gateway.GatewayException.class,
                org.opensearch.gateway.GatewayException::new,
                65,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.shard.IndexShardNotRecoveringException.class,
                org.opensearch.index.shard.IndexShardNotRecoveringException::new,
                66,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.http.HttpException.class,
                org.opensearch.http.HttpException::new,
                67,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(OpenSearchException.class, OpenSearchException::new, 68, UNKNOWN_VERSION_ADDED)
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.snapshots.SnapshotMissingException.class,
                org.opensearch.snapshots.SnapshotMissingException::new,
                69,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.action.PrimaryMissingActionException.class,
                org.opensearch.action.PrimaryMissingActionException::new,
                70,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.search.SearchParseException.class,
                org.opensearch.search.SearchParseException::new,
                72,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.action.FailedNodeException.class,
                org.opensearch.action.FailedNodeException::new,
                71,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.snapshots.ConcurrentSnapshotExecutionException.class,
                org.opensearch.snapshots.ConcurrentSnapshotExecutionException::new,
                73,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.common.blobstore.BlobStoreException.class,
                org.opensearch.common.blobstore.BlobStoreException::new,
                74,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.cluster.IncompatibleClusterStateVersionException.class,
                org.opensearch.cluster.IncompatibleClusterStateVersionException::new,
                75,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.engine.RecoveryEngineException.class,
                org.opensearch.index.engine.RecoveryEngineException::new,
                76,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.common.util.concurrent.UncategorizedExecutionException.class,
                org.opensearch.common.util.concurrent.UncategorizedExecutionException::new,
                77,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.action.TimestampParsingException.class,
                org.opensearch.action.TimestampParsingException::new,
                78,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.action.RoutingMissingException.class,
                org.opensearch.action.RoutingMissingException::new,
                79,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 80 was IndexFailedEngineException, deprecated in 6.0, removed in 7.0
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.snapshots.IndexShardRestoreFailedException.class,
                org.opensearch.index.snapshots.IndexShardRestoreFailedException::new,
                81,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.repositories.RepositoryException.class,
                org.opensearch.repositories.RepositoryException::new,
                82,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.ReceiveTimeoutTransportException.class,
                org.opensearch.transport.ReceiveTimeoutTransportException::new,
                83,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.NodeDisconnectedException.class,
                org.opensearch.transport.NodeDisconnectedException::new,
                84,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 85 used to be for AlreadyExpiredException
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.search.aggregations.AggregationExecutionException.class,
                org.opensearch.search.aggregations.AggregationExecutionException::new,
                86,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 87 used to be for MergeMappingException
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.indices.InvalidIndexTemplateException.class,
                org.opensearch.indices.InvalidIndexTemplateException::new,
                88,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.engine.RefreshFailedEngineException.class,
                org.opensearch.index.engine.RefreshFailedEngineException::new,
                90,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.search.aggregations.AggregationInitializationException.class,
                org.opensearch.search.aggregations.AggregationInitializationException::new,
                91,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.indices.recovery.DelayRecoveryException.class,
                org.opensearch.indices.recovery.DelayRecoveryException::new,
                92,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 93 used to be for IndexWarmerMissingException
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.client.transport.NoNodeAvailableException.class,
                org.opensearch.client.transport.NoNodeAvailableException::new,
                94,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.snapshots.InvalidSnapshotNameException.class,
                org.opensearch.snapshots.InvalidSnapshotNameException::new,
                96,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.shard.IllegalIndexShardStateException.class,
                org.opensearch.index.shard.IllegalIndexShardStateException::new,
                97,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                IndexShardSnapshotException.class,
                IndexShardSnapshotException::new,
                98,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.shard.IndexShardNotStartedException.class,
                org.opensearch.index.shard.IndexShardNotStartedException::new,
                99,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.action.search.SearchPhaseExecutionException.class,
                org.opensearch.action.search.SearchPhaseExecutionException::new,
                100,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.ActionNotFoundTransportException.class,
                org.opensearch.transport.ActionNotFoundTransportException::new,
                101,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.TransportSerializationException.class,
                org.opensearch.transport.TransportSerializationException::new,
                102,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.RemoteTransportException.class,
                org.opensearch.transport.RemoteTransportException::new,
                103,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.engine.EngineCreationFailureException.class,
                org.opensearch.index.engine.EngineCreationFailureException::new,
                104,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.cluster.routing.RoutingException.class,
                org.opensearch.cluster.routing.RoutingException::new,
                105,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.shard.IndexShardRecoveryException.class,
                org.opensearch.index.shard.IndexShardRecoveryException::new,
                106,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.repositories.RepositoryMissingException.class,
                org.opensearch.repositories.RepositoryMissingException::new,
                107,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.engine.DocumentSourceMissingException.class,
                org.opensearch.index.engine.DocumentSourceMissingException::new,
                109,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 110 used to be FlushNotAllowedEngineException
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.common.settings.NoClassSettingsException.class,
                org.opensearch.common.settings.NoClassSettingsException::new,
                111,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.BindTransportException.class,
                org.opensearch.transport.BindTransportException::new,
                112,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.rest.action.admin.indices.AliasesNotFoundException.class,
                org.opensearch.rest.action.admin.indices.AliasesNotFoundException::new,
                113,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.shard.IndexShardRecoveringException.class,
                org.opensearch.index.shard.IndexShardRecoveringException::new,
                114,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.translog.TranslogException.class,
                org.opensearch.index.translog.TranslogException::new,
                115,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.cluster.metadata.ProcessClusterEventTimeoutException.class,
                org.opensearch.cluster.metadata.ProcessClusterEventTimeoutException::new,
                116,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                ReplicationOperation.RetryOnPrimaryException.class,
                ReplicationOperation.RetryOnPrimaryException::new,
                117,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.OpenSearchTimeoutException.class,
                org.opensearch.OpenSearchTimeoutException::new,
                118,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.search.query.QueryPhaseExecutionException.class,
                org.opensearch.search.query.QueryPhaseExecutionException::new,
                119,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.repositories.RepositoryVerificationException.class,
                org.opensearch.repositories.RepositoryVerificationException::new,
                120,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.search.aggregations.InvalidAggregationPathException.class,
                org.opensearch.search.aggregations.InvalidAggregationPathException::new,
                121,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 123 used to be IndexAlreadyExistsException and was renamed
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                ResourceAlreadyExistsException.class,
                ResourceAlreadyExistsException::new,
                123,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 124 used to be Script.ScriptParseException
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                TcpTransport.HttpRequestOnTransportException.class,
                TcpTransport.HttpRequestOnTransportException::new,
                125,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.mapper.MapperParsingException.class,
                org.opensearch.index.mapper.MapperParsingException::new,
                126,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 127 used to be org.opensearch.search.SearchContextException
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.search.builder.SearchSourceBuilderException.class,
                org.opensearch.search.builder.SearchSourceBuilderException::new,
                128,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 129 was EngineClosedException
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.action.NoShardAvailableActionException.class,
                org.opensearch.action.NoShardAvailableActionException::new,
                130,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.action.UnavailableShardsException.class,
                org.opensearch.action.UnavailableShardsException::new,
                131,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.engine.FlushFailedEngineException.class,
                org.opensearch.index.engine.FlushFailedEngineException::new,
                132,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.common.breaker.CircuitBreakingException.class,
                org.opensearch.common.breaker.CircuitBreakingException::new,
                133,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.NodeNotConnectedException.class,
                org.opensearch.transport.NodeNotConnectedException::new,
                134,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.mapper.StrictDynamicMappingException.class,
                org.opensearch.index.mapper.StrictDynamicMappingException::new,
                135,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.action.support.replication.TransportReplicationAction.RetryOnReplicaException.class,
                org.opensearch.action.support.replication.TransportReplicationAction.RetryOnReplicaException::new,
                136,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.indices.TypeMissingException.class,
                org.opensearch.indices.TypeMissingException::new,
                137,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.cluster.coordination.FailedToCommitClusterStateException.class,
                org.opensearch.cluster.coordination.FailedToCommitClusterStateException::new,
                140,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.query.QueryShardException.class,
                org.opensearch.index.query.QueryShardException::new,
                141,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                ShardStateAction.NoLongerPrimaryShardException.class,
                ShardStateAction.NoLongerPrimaryShardException::new,
                142,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.script.ScriptException.class,
                org.opensearch.script.ScriptException::new,
                143,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.cluster.NotClusterManagerException.class,
                org.opensearch.cluster.NotClusterManagerException::new,
                144,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.OpenSearchStatusException.class,
                org.opensearch.OpenSearchStatusException::new,
                145,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.tasks.TaskCancelledException.class,
                org.opensearch.tasks.TaskCancelledException::new,
                146,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.env.ShardLockObtainFailedException.class,
                org.opensearch.env.ShardLockObtainFailedException::new,
                147,
                UNKNOWN_VERSION_ADDED
            )
        );
        // 148 was UnknownNamedObjectException
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                MultiBucketConsumerService.TooManyBucketsException.class,
                MultiBucketConsumerService.TooManyBucketsException::new,
                149,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.cluster.coordination.CoordinationStateRejectedException.class,
                org.opensearch.cluster.coordination.CoordinationStateRejectedException::new,
                150,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.snapshots.SnapshotInProgressException.class,
                org.opensearch.snapshots.SnapshotInProgressException::new,
                151,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.NoSuchRemoteClusterException.class,
                org.opensearch.transport.NoSuchRemoteClusterException::new,
                152,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.seqno.RetentionLeaseAlreadyExistsException.class,
                org.opensearch.index.seqno.RetentionLeaseAlreadyExistsException::new,
                153,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.seqno.RetentionLeaseNotFoundException.class,
                org.opensearch.index.seqno.RetentionLeaseNotFoundException::new,
                154,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.shard.ShardNotInPrimaryModeException.class,
                org.opensearch.index.shard.ShardNotInPrimaryModeException::new,
                155,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException.class,
                org.opensearch.index.seqno.RetentionLeaseInvalidRetainingSeqNoException::new,
                156,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.ingest.IngestProcessorException.class,
                org.opensearch.ingest.IngestProcessorException::new,
                157,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.indices.recovery.PeerRecoveryNotFound.class,
                org.opensearch.indices.recovery.PeerRecoveryNotFound::new,
                158,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.cluster.coordination.NodeHealthCheckFailureException.class,
                org.opensearch.cluster.coordination.NodeHealthCheckFailureException::new,
                159,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.transport.NoSeedNodeLeftException.class,
                org.opensearch.transport.NoSeedNodeLeftException::new,
                160,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.indices.replication.common.ReplicationFailedException.class,
                org.opensearch.indices.replication.common.ReplicationFailedException::new,
                161,
                V_2_1_0
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.index.shard.PrimaryShardClosedException.class,
                org.opensearch.index.shard.PrimaryShardClosedException::new,
                162,
                V_3_0_0
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.cluster.decommission.DecommissioningFailedException.class,
                org.opensearch.cluster.decommission.DecommissioningFailedException::new,
                163,
                V_2_4_0
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.cluster.decommission.NodeDecommissionedException.class,
                org.opensearch.cluster.decommission.NodeDecommissionedException::new,
                164,
                V_3_0_0
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                ClusterManagerThrottlingException.class,
                ClusterManagerThrottlingException::new,
                165,
                Version.V_2_5_0
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                SnapshotInUseDeletionException.class,
                SnapshotInUseDeletionException::new,
                166,
                UNKNOWN_VERSION_ADDED
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                UnsupportedWeightedRoutingStateException.class,
                UnsupportedWeightedRoutingStateException::new,
                167,
                V_2_5_0
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                PreferenceBasedSearchNotAllowedException.class,
                PreferenceBasedSearchNotAllowedException::new,
                168,
                V_2_6_0
            )
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(NodeWeighedAwayException.class, NodeWeighedAwayException::new, 169, V_2_6_0)
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(SearchPipelineProcessingException.class, SearchPipelineProcessingException::new, 170, V_2_7_0)
        );
        registerExceptionHandle(
            new BaseOpenSearchExceptionHandle(
                org.opensearch.cluster.block.IndexCreateBlockException.class,
                org.opensearch.cluster.block.IndexCreateBlockException::new,
                CUSTOM_ELASTICSEARCH_EXCEPTIONS_BASE_ID + 1,
                V_3_0_0
            )
        );
    }

    /**
     * Construct a <code>OpenSearchException</code> with the specified cause exception.
     */
    public OpenSearchException(Throwable cause) {
        super(cause);
    }

    /**
     * Construct a <code>OpenSearchException</code> with the specified detail message.
     *
     * The message can be parameterized using <code>{}</code> as placeholders for the given
     * arguments
     *
     * @param msg  the detail message
     * @param args the arguments for the message
     */
    public OpenSearchException(String msg, Object... args) {
        super(msg, args);
    }

    /**
     * Construct a <code>OpenSearchException</code> with the specified detail message
     * and nested exception.
     *
     * The message can be parameterized using <code>{}</code> as placeholders for the given
     * arguments
     *
     * @param msg   the detail message
     * @param cause the nested exception
     * @param args  the arguments for the message
     */
    public OpenSearchException(String msg, Throwable cause, Object... args) {
        super(msg, cause, args);
    }

    public OpenSearchException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    /**
     * Returns <code>true</code> iff the given class is a registered for an exception to be read.
     *
     * note: this "override" method is provided to ensure static registration of the exceptions above
     */
    public static boolean isRegistered(final Class<? extends Throwable> exception, Version version) {
        return BaseOpenSearchException.isRegistered(exception, version);
    }
}
