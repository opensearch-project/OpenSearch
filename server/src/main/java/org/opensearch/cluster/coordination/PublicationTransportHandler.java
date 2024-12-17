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

package org.opensearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.IncompatibleClusterStateVersionException;
import org.opensearch.cluster.coordination.PersistedStateRegistry.PersistedStateType;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.TriConsumer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.gateway.GatewayMetaState.RemotePersistedState;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.BytesTransportRequest;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportException;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportResponseHandler;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Transport handler for publication
 *
 * @opensearch.internal
 */
public class PublicationTransportHandler {

    private static final Logger logger = LogManager.getLogger(PublicationTransportHandler.class);

    public static final String PUBLISH_STATE_ACTION_NAME = "internal:cluster/coordination/publish_state";
    public static final String PUBLISH_REMOTE_STATE_ACTION_NAME = "internal:cluster/coordination/publish_remote_state";
    public static final String COMMIT_STATE_ACTION_NAME = "internal:cluster/coordination/commit_state";

    private final TransportService transportService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest;

    private final AtomicReference<ClusterState> lastSeenClusterState = new AtomicReference<>();

    // the cluster-manager needs the original non-serialized state as the cluster state contains some volatile information that we
    // don't want to be replicated because it's not usable on another node (e.g. UnassignedInfo.unassignedTimeNanos) or
    // because it's mostly just debugging info that would unnecessarily blow up CS updates (I think there was one in
    // snapshot code).
    // TODO: look into these and check how to get rid of them
    private final AtomicReference<PublishRequest> currentPublishRequestToSelf = new AtomicReference<>();

    private final AtomicLong fullClusterStateReceivedCount = new AtomicLong();
    private final AtomicLong incompatibleClusterStateDiffReceivedCount = new AtomicLong();
    private final AtomicLong compatibleClusterStateDiffReceivedCount = new AtomicLong();
    private final AtomicBoolean allNodesRemotePublicationEnabled = new AtomicBoolean();
    // -> no need to put a timeout on the options here, because we want the response to eventually be received
    // and not log an error if it arrives after the timeout
    private final TransportRequestOptions stateRequestOptions = TransportRequestOptions.builder()
        .withType(TransportRequestOptions.Type.STATE)
        .build();
    private final RemoteClusterStateService remoteClusterStateService;

    public PublicationTransportHandler(
        TransportService transportService,
        NamedWriteableRegistry namedWriteableRegistry,
        Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest,
        TriConsumer<ApplyCommitRequest, Consumer<ClusterState>, ActionListener<Void>> handleApplyCommit,
        RemoteClusterStateService remoteClusterStateService
    ) {
        this.transportService = transportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.handlePublishRequest = handlePublishRequest;
        this.remoteClusterStateService = remoteClusterStateService;

        transportService.registerRequestHandler(
            PUBLISH_STATE_ACTION_NAME,
            ThreadPool.Names.GENERIC,
            false,
            false,
            BytesTransportRequest::new,
            (request, channel, task) -> channel.sendResponse(handleIncomingPublishRequest(request))
        );

        transportService.registerRequestHandler(
            PUBLISH_REMOTE_STATE_ACTION_NAME,
            ThreadPool.Names.GENERIC,
            false,
            false,
            RemotePublishRequest::new,
            (request, channel, task) -> channel.sendResponse(handleIncomingRemotePublishRequest(request))
        );

        transportService.registerRequestHandler(
            COMMIT_STATE_ACTION_NAME,
            ThreadPool.Names.GENERIC,
            false,
            false,
            ApplyCommitRequest::new,
            (request, channel, task) -> handleApplyCommit.apply(request, this::updateLastSeen, transportCommitCallback(channel))
        );
    }

    private ActionListener<Void> transportCommitCallback(TransportChannel channel) {
        return new ActionListener<Void>() {

            @Override
            public void onResponse(Void aVoid) {
                try {
                    channel.sendResponse(TransportResponse.Empty.INSTANCE);
                } catch (IOException e) {
                    logger.debug("failed to send response on commit", e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    channel.sendResponse(e);
                } catch (IOException ie) {
                    e.addSuppressed(ie);
                    logger.debug("failed to send response on commit", e);
                }
            }
        };
    }

    public PublishClusterStateStats stats() {
        return new PublishClusterStateStats(
            fullClusterStateReceivedCount.get(),
            incompatibleClusterStateDiffReceivedCount.get(),
            compatibleClusterStateDiffReceivedCount.get()
        );
    }

    private PublishWithJoinResponse handleIncomingPublishRequest(BytesTransportRequest request) throws IOException {
        try (StreamInput in = CompressedStreamUtils.decompressBytes(request, namedWriteableRegistry)) {
            ClusterState incomingState;
            if (in.readBoolean()) {
                // Close early to release resources used by the de-compression as early as possible
                try (StreamInput input = in) {
                    incomingState = ClusterState.readFrom(input, transportService.getLocalNode());
                } catch (Exception e) {
                    logger.warn("unexpected error while deserializing an incoming cluster state", e);
                    throw e;
                }
                fullClusterStateReceivedCount.incrementAndGet();
                logger.debug("received full cluster state version [{}] with size [{}]", incomingState.version(), request.bytes().length());
                final PublishWithJoinResponse response = acceptState(incomingState, null);
                lastSeenClusterState.set(incomingState);
                return response;
            } else {
                final ClusterState lastSeen = lastSeenClusterState.get();
                if (lastSeen == null) {
                    logger.debug("received diff for but don't have any local cluster state - requesting full state");
                    incompatibleClusterStateDiffReceivedCount.incrementAndGet();
                    throw new IncompatibleClusterStateVersionException("have no local cluster state");
                } else {
                    try {
                        final Diff<ClusterState> diff;
                        // Close stream early to release resources used by the de-compression as early as possible
                        try (StreamInput input = in) {
                            diff = ClusterState.readDiffFrom(input, lastSeen.nodes().getLocalNode());
                        }
                        incomingState = diff.apply(lastSeen); // might throw IncompatibleClusterStateVersionException
                    } catch (IncompatibleClusterStateVersionException e) {
                        incompatibleClusterStateDiffReceivedCount.incrementAndGet();
                        throw e;
                    } catch (Exception e) {
                        logger.warn("unexpected error while deserializing an incoming cluster state", e);
                        throw e;
                    }
                    compatibleClusterStateDiffReceivedCount.incrementAndGet();
                    logger.debug(
                        "received diff cluster state version [{}] with uuid [{}], diff size [{}]",
                        incomingState.version(),
                        incomingState.stateUUID(),
                        request.bytes().length()
                    );
                    final PublishWithJoinResponse response = acceptState(incomingState, null);
                    lastSeenClusterState.compareAndSet(lastSeen, incomingState);
                    return response;
                }
            }
        }
    }

    // package private for testing
    PublishWithJoinResponse handleIncomingRemotePublishRequest(RemotePublishRequest request) throws IOException, IllegalStateException {
        boolean applyFullState = false;
        try {
            if (transportService.getLocalNode().equals(request.getSourceNode())) {
                return acceptRemoteStateOnLocalNode(request);
            }
            // TODO Make cluster state download non-blocking: https://github.com/opensearch-project/OpenSearch/issues/14102
            ClusterMetadataManifest manifest = remoteClusterStateService.getClusterMetadataManifestByFileName(
                request.getClusterUUID(),
                request.getManifestFile()
            );
            if (manifest == null) {
                throw new IllegalStateException("Publication failed as manifest was not found for " + request);
            }
            final ClusterState lastSeen = lastSeenClusterState.get();
            if (lastSeen == null) {
                logger.debug(() -> "Diff cannot be applied as there is no last cluster state");
                applyFullState = true;
            } else if (manifest.getDiffManifest() == null) {
                logger.debug(() -> "There is no diff in the manifest");
                applyFullState = true;
            } else if (manifest.getDiffManifest().getFromStateUUID().equals(lastSeen.stateUUID()) == false) {
                logger.debug(() -> "Last cluster state not compatible with the diff");
                applyFullState = true;
            }

            if (applyFullState == true) {
                logger.debug(
                    () -> new ParameterizedMessage(
                        "Downloading full cluster state for term {}, version {}, stateUUID {}",
                        manifest.getClusterTerm(),
                        manifest.getStateVersion(),
                        manifest.getStateUUID()
                    )
                );
                ClusterState clusterState = remoteClusterStateService.getClusterStateForManifest(
                    request.getClusterName(),
                    manifest,
                    transportService.getLocalNode().getId(),
                    true
                );
                fullClusterStateReceivedCount.incrementAndGet();
                final PublishWithJoinResponse response = acceptState(clusterState, manifest);
                lastSeenClusterState.set(clusterState);
                return response;
            } else {
                logger.debug(
                    () -> new ParameterizedMessage(
                        "Downloading diff cluster state for term {}, version {}, previousUUID {}, current UUID {}",
                        manifest.getClusterTerm(),
                        manifest.getStateVersion(),
                        manifest.getDiffManifest().getFromStateUUID(),
                        manifest.getStateUUID()
                    )
                );
                ClusterState clusterState = remoteClusterStateService.getClusterStateUsingDiff(
                    manifest,
                    lastSeen,
                    transportService.getLocalNode().getId()
                );
                compatibleClusterStateDiffReceivedCount.incrementAndGet();
                final PublishWithJoinResponse response = acceptState(clusterState, manifest);
                lastSeenClusterState.compareAndSet(lastSeen, clusterState);
                return response;
            }
        } catch (Exception e) {
            if (applyFullState) {
                remoteClusterStateService.fullIncomingPublicationFailed();
            } else {
                remoteClusterStateService.diffIncomingPublicationFailed();
            }
            throw e;
        }
    }

    private PublishWithJoinResponse acceptState(ClusterState incomingState, ClusterMetadataManifest manifest) {
        // if the state is coming from the current node, use original request instead (see currentPublishRequestToSelf for explanation)
        if (transportService.getLocalNode().equals(incomingState.nodes().getClusterManagerNode())) {
            final PublishRequest publishRequest = currentPublishRequestToSelf.get();
            if (publishRequest == null || publishRequest.getAcceptedState().stateUUID().equals(incomingState.stateUUID()) == false) {
                throw new IllegalStateException("publication to self failed for " + publishRequest);
            } else {
                return handlePublishRequest.apply(publishRequest);
            }
        }
        if (manifest != null) {
            return handlePublishRequest.apply(new RemoteStatePublishRequest(incomingState, manifest));
        }
        return handlePublishRequest.apply(new PublishRequest(incomingState));
    }

    private PublishWithJoinResponse acceptRemoteStateOnLocalNode(RemotePublishRequest remotePublishRequest) {
        final PublishRequest publishRequest = currentPublishRequestToSelf.get();
        if (publishRequest == null
            || publishRequest.getAcceptedState().coordinationMetadata().term() != remotePublishRequest.term
            || publishRequest.getAcceptedState().version() != remotePublishRequest.version) {
            logger.debug(
                () -> new ParameterizedMessage(
                    "Publication failure for current publish request : {} and remote publish request: {}",
                    publishRequest,
                    remotePublishRequest
                )
            );
            throw new IllegalStateException("publication to self failed for " + remotePublishRequest);
        }
        PublishWithJoinResponse publishWithJoinResponse = handlePublishRequest.apply(publishRequest);
        lastSeenClusterState.set(publishRequest.getAcceptedState());
        return publishWithJoinResponse;
    }

    public PublicationContext newPublicationContext(
        ClusterChangedEvent clusterChangedEvent,
        boolean isRemotePublicationEnabled,
        PersistedStateRegistry persistedStateRegistry
    ) {
        if (isRemotePublicationEnabled == true) {
            if (allNodesRemotePublicationEnabled.get() == false) {
                if (validateRemotePublicationConfiguredOnAllNodes(clusterChangedEvent.state().nodes()) == true) {
                    allNodesRemotePublicationEnabled.set(true);
                }
            }
            if (allNodesRemotePublicationEnabled.get() == true) {
                // if all nodes are remote then create remote publication context
                return new RemotePublicationContext(clusterChangedEvent, persistedStateRegistry);
            }
        }
        final PublicationContext publicationContext = new PublicationContext(clusterChangedEvent, persistedStateRegistry);

        // Build the serializations we expect to need now, early in the process, so that an error during serialization fails the publication
        // straight away. This isn't watertight since we send diffs on a best-effort basis and may fall back to sending a full state (and
        // therefore serializing it) if the diff-based publication fails.
        publicationContext.buildDiffAndSerializeStates();
        return publicationContext;
    }

    private boolean validateRemotePublicationConfiguredOnAllNodes(DiscoveryNodes discoveryNodes) {
        for (DiscoveryNode node : discoveryNodes.getNodes().values()) {
            // if a node is non-remote then created local publication context
            if (node.isRemoteStatePublicationEnabled() == false) {
                return false;
            }
        }
        return true;
    }

    private void updateLastSeen(final ClusterState clusterState) {
        lastSeenClusterState.set(clusterState);
    }

    // package private for testing
    void setCurrentPublishRequestToSelf(PublishRequest publishRequest) {
        this.currentPublishRequestToSelf.set(publishRequest);
    }

    // package private for testing
    void setLastSeenClusterState(ClusterState clusterState) {
        this.lastSeenClusterState.set(clusterState);
    }

    private static BytesReference serializeFullClusterState(ClusterState clusterState, Version nodeVersion) throws IOException {
        final BytesReference serializedState = CompressedStreamUtils.createCompressedStream(nodeVersion, stream -> {
            stream.writeBoolean(true);
            clusterState.writeTo(stream);
        });
        logger.trace(
            "serialized full cluster state version [{}] for node version [{}] with size [{}]",
            clusterState.version(),
            nodeVersion,
            serializedState.length()
        );
        return serializedState;
    }

    private static BytesReference serializeDiffClusterState(Diff<ClusterState> diff, Version nodeVersion) throws IOException {
        return CompressedStreamUtils.createCompressedStream(nodeVersion, stream -> {
            stream.writeBoolean(false);
            diff.writeTo(stream);
        });
    }

    /**
     * Publishing a cluster state typically involves sending the same cluster state (or diff) to every node, so the work of diffing,
     * serializing, and compressing the state can be done once and the results shared across publish requests. The
     * {@code PublicationContext} implements this sharing.
     *
     * @opensearch.internal
     */
    public class PublicationContext {

        protected final DiscoveryNodes discoveryNodes;
        protected final ClusterState newState;
        protected final ClusterState previousState;
        protected final boolean sendFullVersion;
        private final Map<Version, BytesReference> serializedStates = new HashMap<>();
        private final Map<Version, BytesReference> serializedDiffs = new HashMap<>();
        protected final PersistedStateRegistry persistedStateRegistry;

        PublicationContext(ClusterChangedEvent clusterChangedEvent, PersistedStateRegistry persistedStateRegistry) {
            discoveryNodes = clusterChangedEvent.state().nodes();
            newState = clusterChangedEvent.state();
            previousState = clusterChangedEvent.previousState();
            sendFullVersion = previousState.getBlocks().disableStatePersistence();
            this.persistedStateRegistry = persistedStateRegistry;
        }

        void buildDiffAndSerializeStates() {
            Diff<ClusterState> diff = null;
            for (DiscoveryNode node : discoveryNodes) {
                try {
                    if (sendFullVersion || previousState.nodes().nodeExists(node) == false) {
                        if (serializedStates.containsKey(node.getVersion()) == false) {
                            serializedStates.put(node.getVersion(), serializeFullClusterState(newState, node.getVersion()));
                        }
                    } else {
                        // will send a diff
                        if (diff == null) {
                            diff = newState.diff(previousState);
                        }
                        if (serializedDiffs.containsKey(node.getVersion()) == false) {
                            final BytesReference serializedDiff = serializeDiffClusterState(diff, node.getVersion());
                            serializedDiffs.put(node.getVersion(), serializedDiff);
                            logger.trace(
                                "serialized cluster state diff for version [{}] in for node version [{}] with size [{}]",
                                newState.version(),
                                node.getVersion(),
                                serializedDiff.length()
                            );
                        }
                    }
                } catch (IOException e) {
                    throw new OpenSearchException("failed to serialize cluster state for publishing to node {}", e, node);
                }
            }
        }

        public void sendPublishRequest(
            DiscoveryNode destination,
            PublishRequest publishRequest,
            ActionListener<PublishWithJoinResponse> listener
        ) {
            assert publishRequest.getAcceptedState() == newState : "state got switched on us";
            assert transportService.getThreadPool().getThreadContext().isSystemContext();
            final ActionListener<PublishWithJoinResponse> responseActionListener;
            if (destination.equals(discoveryNodes.getLocalNode())) {
                // if publishing to self, use original request instead (see currentPublishRequestToSelf for explanation)
                final PublishRequest previousRequest = currentPublishRequestToSelf.getAndSet(publishRequest);
                // we might override an in-flight publication to self in case where we failed as cluster-manager and
                // became cluster-manager again, and the new publication started before the previous one completed
                // (which fails anyhow because of higher current term)
                assert previousRequest == null || previousRequest.getAcceptedState().term() < publishRequest.getAcceptedState().term();
                responseActionListener = new ActionListener<PublishWithJoinResponse>() {
                    @Override
                    public void onResponse(PublishWithJoinResponse publishWithJoinResponse) {
                        currentPublishRequestToSelf.compareAndSet(publishRequest, null); // only clean-up our mess
                        listener.onResponse(publishWithJoinResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        currentPublishRequestToSelf.compareAndSet(publishRequest, null); // only clean-up our mess
                        listener.onFailure(e);
                    }
                };
            } else {
                responseActionListener = listener;
            }
            sendClusterState(destination, responseActionListener);
        }

        public void sendApplyCommit(
            DiscoveryNode destination,
            ApplyCommitRequest applyCommitRequest,
            ActionListener<TransportResponse.Empty> listener
        ) {
            assert transportService.getThreadPool().getThreadContext().isSystemContext();
            transportService.sendRequest(
                destination,
                COMMIT_STATE_ACTION_NAME,
                applyCommitRequest,
                stateRequestOptions,
                new TransportResponseHandler<TransportResponse.Empty>() {

                    @Override
                    public TransportResponse.Empty read(StreamInput in) {
                        return TransportResponse.Empty.INSTANCE;
                    }

                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        listener.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }
                }
            );
        }

        public void sendClusterState(DiscoveryNode destination, ActionListener<PublishWithJoinResponse> listener) {
            logger.trace("sending cluster state over transport to node: {}", destination.getName());
            if (sendFullVersion || previousState.nodes().nodeExists(destination) == false) {
                logger.trace("sending full cluster state version [{}] to [{}]", newState.version(), destination);
                sendFullClusterState(destination, listener);
            } else {
                logger.trace("sending cluster state diff for version [{}] to [{}]", newState.version(), destination);
                sendClusterStateDiff(destination, listener);
            }
        }

        private void sendFullClusterState(DiscoveryNode destination, ActionListener<PublishWithJoinResponse> listener) {
            BytesReference bytes = serializedStates.get(destination.getVersion());
            if (bytes == null) {
                try {
                    bytes = serializeFullClusterState(newState, destination.getVersion());
                    serializedStates.put(destination.getVersion(), bytes);
                } catch (Exception e) {
                    logger.warn(
                        () -> new ParameterizedMessage("failed to serialize cluster state before publishing it to node {}", destination),
                        e
                    );
                    listener.onFailure(e);
                    return;
                }
            }
            sendClusterState(destination, bytes, false, listener);
        }

        private void sendClusterStateDiff(DiscoveryNode destination, ActionListener<PublishWithJoinResponse> listener) {
            final BytesReference bytes = serializedDiffs.get(destination.getVersion());
            assert bytes != null : "failed to find serialized diff for node "
                + destination
                + " of version ["
                + destination.getVersion()
                + "]";
            sendClusterState(destination, bytes, true, listener);
        }

        private void sendClusterState(
            DiscoveryNode destination,
            BytesReference bytes,
            boolean retryWithFullClusterStateOnFailure,
            ActionListener<PublishWithJoinResponse> listener
        ) {
            try {
                final BytesTransportRequest request = new BytesTransportRequest(bytes, destination.getVersion());
                final Consumer<TransportException> transportExceptionHandler = exp -> {
                    if (retryWithFullClusterStateOnFailure && exp.unwrapCause() instanceof IncompatibleClusterStateVersionException) {
                        logger.debug("resending full cluster state to node {} reason {}", destination, exp.getDetailedMessage());
                        sendFullClusterState(destination, listener);
                    } else {
                        logger.debug(() -> new ParameterizedMessage("failed to send cluster state to {}", destination), exp);
                        listener.onFailure(exp);
                    }
                };
                final TransportResponseHandler<PublishWithJoinResponse> responseHandler = new TransportResponseHandler<
                    PublishWithJoinResponse>() {

                    @Override
                    public PublishWithJoinResponse read(StreamInput in) throws IOException {
                        return new PublishWithJoinResponse(in);
                    }

                    @Override
                    public void handleResponse(PublishWithJoinResponse response) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        transportExceptionHandler.accept(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }
                };
                transportService.sendRequest(destination, PUBLISH_STATE_ACTION_NAME, request, stateRequestOptions, responseHandler);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("error sending cluster state to {}", destination), e);
                listener.onFailure(e);
            }
        }
    }

    /**
     * An extension of {@code PublicationContext} to support remote cluster state publication
     *
     * @opensearch.internal
     */
    public class RemotePublicationContext extends PublicationContext {

        RemotePublicationContext(ClusterChangedEvent clusterChangedEvent, PersistedStateRegistry persistedStateRegistry) {
            super(clusterChangedEvent, persistedStateRegistry);
        }

        @Override
        public void sendClusterState(final DiscoveryNode destination, final ActionListener<PublishWithJoinResponse> listener) {
            try {
                logger.debug("sending remote cluster state to node: {}", destination.getName());
                final String manifestFileName = ((RemotePersistedState) persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE))
                    .getLastUploadedManifestFile();
                final RemotePublishRequest remotePublishRequest = new RemotePublishRequest(
                    discoveryNodes.getLocalNode(),
                    newState.term(),
                    newState.getVersion(),
                    newState.getClusterName().value(),
                    newState.metadata().clusterUUID(),
                    manifestFileName
                );
                final Consumer<TransportException> transportExceptionHandler = exp -> {
                    logger.debug(() -> new ParameterizedMessage("failed to send remote cluster state to {}", destination), exp);
                    listener.onFailure(exp);
                };
                final TransportResponseHandler<PublishWithJoinResponse> responseHandler = new TransportResponseHandler<>() {

                    @Override
                    public PublishWithJoinResponse read(StreamInput in) throws IOException {
                        return new PublishWithJoinResponse(in);
                    }

                    @Override
                    public void handleResponse(PublishWithJoinResponse response) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        transportExceptionHandler.accept(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.GENERIC;
                    }
                };
                transportService.sendRequest(
                    destination,
                    PUBLISH_REMOTE_STATE_ACTION_NAME,
                    remotePublishRequest,
                    stateRequestOptions,
                    responseHandler
                );
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("error sending remote cluster state to {}", destination), e);
                listener.onFailure(e);
            }
        }
    }

}
