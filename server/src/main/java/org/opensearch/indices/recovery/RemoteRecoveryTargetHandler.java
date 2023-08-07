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

package org.opensearch.indices.recovery;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.replication.RemoteSegmentFileChunkWriter;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.transport.EmptyTransportResponseHandler;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Target handler for remote recovery
 *
 * @opensearch.internal
 */
public class RemoteRecoveryTargetHandler implements RecoveryTargetHandler {

    private static final Logger logger = LogManager.getLogger(RemoteRecoveryTargetHandler.class);

    private final TransportService transportService;
    private final long recoveryId;
    private final ShardId shardId;
    private final DiscoveryNode targetNode;
    private final RecoverySettings recoverySettings;

    private final TransportRequestOptions translogOpsRequestOptions;

    private final AtomicLong requestSeqNoGenerator = new AtomicLong(0);
    private final RetryableTransportClient retryableTransportClient;
    private final RemoteSegmentFileChunkWriter fileChunkWriter;

    public RemoteRecoveryTargetHandler(
        long recoveryId,
        ShardId shardId,
        TransportService transportService,
        DiscoveryNode targetNode,
        RecoverySettings recoverySettings,
        Consumer<Long> onSourceThrottle
    ) {
        this.transportService = transportService;
        // It is safe to pass the retry timeout value here because RemoteRecoveryTargetHandler
        // created per recovery. Any change to RecoverySettings will be applied on the next
        // recovery.
        this.retryableTransportClient = new RetryableTransportClient(
            transportService,
            targetNode,
            recoverySettings.internalActionRetryTimeout(),
            logger
        );
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.targetNode = targetNode;
        this.recoverySettings = recoverySettings;
        this.translogOpsRequestOptions = TransportRequestOptions.builder()
            .withType(TransportRequestOptions.Type.RECOVERY)
            .withTimeout(recoverySettings.internalActionLongTimeout())
            .build();
        this.fileChunkWriter = new RemoteSegmentFileChunkWriter(
            recoveryId,
            recoverySettings,
            retryableTransportClient,
            shardId,
            PeerRecoveryTargetService.Actions.FILE_CHUNK,
            requestSeqNoGenerator,
            onSourceThrottle
        );
    }

    public DiscoveryNode targetNode() {
        return targetNode;
    }

    @Override
    public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
        final String action = PeerRecoveryTargetService.Actions.PREPARE_TRANSLOG;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        final RecoveryPrepareForTranslogOperationsRequest request = new RecoveryPrepareForTranslogOperationsRequest(
            recoveryId,
            requestSeqNo,
            shardId,
            totalTranslogOps
        );
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        final ActionListener<TransportResponse.Empty> responseListener = ActionListener.map(listener, r -> null);
        retryableTransportClient.executeRetryableAction(action, request, responseListener, reader);
    }

    @Override
    public void finalizeRecovery(final long globalCheckpoint, final long trimAboveSeqNo, final ActionListener<Void> listener) {
        final String action = PeerRecoveryTargetService.Actions.FINALIZE;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        final RecoveryFinalizeRecoveryRequest request = new RecoveryFinalizeRecoveryRequest(
            recoveryId,
            requestSeqNo,
            shardId,
            globalCheckpoint,
            trimAboveSeqNo
        );
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        final ActionListener<TransportResponse.Empty> responseListener = ActionListener.map(listener, r -> null);
        retryableTransportClient.executeRetryableAction(action, request, responseListener, reader);
    }

    @Override
    public void handoffPrimaryContext(final ReplicationTracker.PrimaryContext primaryContext) {
        transportService.submitRequest(
            targetNode,
            PeerRecoveryTargetService.Actions.HANDOFF_PRIMARY_CONTEXT,
            new RecoveryHandoffPrimaryContextRequest(recoveryId, shardId, primaryContext),
            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionTimeout()).build(),
            EmptyTransportResponseHandler.INSTANCE_SAME
        ).txGet();
    }

    @Override
    public void indexTranslogOperations(
        final List<Translog.Operation> operations,
        final int totalTranslogOps,
        final long maxSeenAutoIdTimestampOnPrimary,
        final long maxSeqNoOfDeletesOrUpdatesOnPrimary,
        final RetentionLeases retentionLeases,
        final long mappingVersionOnPrimary,
        final ActionListener<Long> listener
    ) {
        final String action = PeerRecoveryTargetService.Actions.TRANSLOG_OPS;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        final RecoveryTranslogOperationsRequest request = new RecoveryTranslogOperationsRequest(
            recoveryId,
            requestSeqNo,
            shardId,
            operations,
            totalTranslogOps,
            maxSeenAutoIdTimestampOnPrimary,
            maxSeqNoOfDeletesOrUpdatesOnPrimary,
            retentionLeases,
            mappingVersionOnPrimary
        );
        final Writeable.Reader<RecoveryTranslogOperationsResponse> reader = RecoveryTranslogOperationsResponse::new;
        final ActionListener<RecoveryTranslogOperationsResponse> responseListener = ActionListener.map(listener, r -> r.localCheckpoint);
        retryableTransportClient.executeRetryableAction(action, request, translogOpsRequestOptions, responseListener, reader);
    }

    /**
     * Used with Segment replication only
     *
     * This function is used to force a sync target primary node with source (old primary). This is to avoid segment files
     * conflict with replicas when target is promoted as primary.
     */
    @Override
    public void forceSegmentFileSync() {
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        transportService.submitRequest(
            targetNode,
            SegmentReplicationTargetService.Actions.FORCE_SYNC,
            new ForceSyncRequest(requestSeqNo, recoveryId, shardId),
            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionLongTimeout()).build(),
            EmptyTransportResponseHandler.INSTANCE_SAME
        ).txGet();
    }

    @Override
    public void receiveFileInfo(
        List<String> phase1FileNames,
        List<Long> phase1FileSizes,
        List<String> phase1ExistingFileNames,
        List<Long> phase1ExistingFileSizes,
        int totalTranslogOps,
        ActionListener<Void> listener
    ) {
        final String action = PeerRecoveryTargetService.Actions.FILES_INFO;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        RecoveryFilesInfoRequest request = new RecoveryFilesInfoRequest(
            recoveryId,
            requestSeqNo,
            shardId,
            phase1FileNames,
            phase1FileSizes,
            phase1ExistingFileNames,
            phase1ExistingFileSizes,
            totalTranslogOps
        );
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        final ActionListener<TransportResponse.Empty> responseListener = ActionListener.map(listener, r -> null);
        retryableTransportClient.executeRetryableAction(action, request, responseListener, reader);
    }

    @Override
    public void cleanFiles(
        int totalTranslogOps,
        long globalCheckpoint,
        Store.MetadataSnapshot sourceMetadata,
        ActionListener<Void> listener
    ) {
        final String action = PeerRecoveryTargetService.Actions.CLEAN_FILES;
        final long requestSeqNo = requestSeqNoGenerator.getAndIncrement();
        final RecoveryCleanFilesRequest request = new RecoveryCleanFilesRequest(
            recoveryId,
            requestSeqNo,
            shardId,
            sourceMetadata,
            totalTranslogOps,
            globalCheckpoint
        );
        final Writeable.Reader<TransportResponse.Empty> reader = in -> TransportResponse.Empty.INSTANCE;
        final ActionListener<TransportResponse.Empty> responseListener = ActionListener.map(listener, r -> null);
        retryableTransportClient.executeRetryableAction(action, request, responseListener, reader);
    }

    @Override
    public void cancel() {
        retryableTransportClient.cancel();
    }

    @Override
    public void writeFileChunk(
        StoreFileMetadata fileMetadata,
        long position,
        BytesReference content,
        boolean lastChunk,
        int totalTranslogOps,
        ActionListener<Void> listener
    ) {
        fileChunkWriter.writeFileChunk(fileMetadata, position, content, lastChunk, totalTranslogOps, listener);
    }
}
