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
import org.opensearch.cluster.metadata.IndexMetadata;
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
import org.opensearch.gateway.remote.IndexMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Transport handler for publication
 *
 * @opensearch.internal
 */
public class IndexMetadataPublicationTransportHandler {

    private static final Logger logger = LogManager.getLogger(IndexMetadataPublicationTransportHandler.class);

    public static final String PUBLISH_INDEX_METADATA_STATE_ACTION_NAME = "internal:cluster/coordination/publish_index_metadata_state";

    private final TransportService transportService;
    private final NamedWriteableRegistry namedWriteableRegistry;
    private final BiFunction<Map<String, IndexMetadata>, Integer, IndexMetadataPublishResponse> handleIndexMetadataPublishRequest;

    private final AtomicReference<Map<String, IndexMetadata>> lastSeenIndexMetadata = new AtomicReference<>();

    // -> no need to put a timeout on the options here, because we want the response to eventually be received
    // and not log an error if it arrives after the timeout
    private final TransportRequestOptions stateRequestOptions = TransportRequestOptions.builder()
        .withType(TransportRequestOptions.Type.STATE)
        .build();
    private final RemoteClusterStateService remoteClusterStateService;

    public IndexMetadataPublicationTransportHandler(
        TransportService transportService,
        NamedWriteableRegistry namedWriteableRegistry,
        BiFunction<Map<String, IndexMetadata>, Integer, IndexMetadataPublishResponse> handlePublishRequest,
        RemoteClusterStateService remoteClusterStateService
    ) {
        this.transportService = transportService;
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.handleIndexMetadataPublishRequest = handlePublishRequest;
        this.remoteClusterStateService = remoteClusterStateService;

        transportService.registerRequestHandler(
            PUBLISH_INDEX_METADATA_STATE_ACTION_NAME,
            ThreadPool.Names.GENERIC,
            false,
            false,
            IndexMetadataPublishRequest::new,
            (request, channel, task) -> channel.sendResponse(handleIncomingRemotePublishRequest(request))
        );
    }

    // package private for testing
    IndexMetadataPublishResponse handleIncomingRemotePublishRequest(IndexMetadataPublishRequest request) throws IOException, IllegalStateException {
        IndexMetadataManifest indexManifest = remoteClusterStateService.getLatestIndexMetadataManifest();
        boolean applyFullIndexMetadataState = false;

        final Map<String, IndexMetadata> lastSeen = lastSeenIndexMetadata.get();
        if (lastSeen == null) {
            logger.info(() -> "Diff cannot be applied as there is no last cluster state");
            applyFullIndexMetadataState = true;
        } else if (indexManifest.getIndexDiffManifest() == null) {
            logger.info(() -> "There is no diff in the manifest");
            applyFullIndexMetadataState = true;
        }

        final Map<String, IndexMetadata> latestIndices;

        if (applyFullIndexMetadataState == true) {
            latestIndices = remoteClusterStateService.getIndexMetadataFromManifest(indexManifest);
        } else {
            latestIndices = remoteClusterStateService.getIndexMetadataStateUsingDiff(
                indexManifest,
                lastSeen
            );
        }

        logger.info("Fetched latest manifest. Contains indices - " + indexManifest.getIndices().size());

        return handleIndexMetadataPublishRequest.apply(latestIndices, indexManifest.getManifestVersion());
    }


    public IndexMetadataPublicationContext newIndexMetadataPublicationContext(
        ClusterState clusterState,
        PersistedStateRegistry persistedStateRegistry
    ) {
        return new IndexMetadataPublicationContext(clusterState, persistedStateRegistry);
    }

    public class IndexMetadataPublicationContext {

        protected final DiscoveryNodes discoveryNodes;
        protected final ClusterState newState;
        protected final PersistedStateRegistry persistedStateRegistry;

        IndexMetadataPublicationContext(ClusterState clusterState, PersistedStateRegistry persistedStateRegistry) {
            discoveryNodes = clusterState.nodes();
            newState = clusterState;
            this.persistedStateRegistry = persistedStateRegistry;
        }

        public void sendPublishRequest(
            DiscoveryNode destination,
            ActionListener<IndexMetadataPublishResponse> listener
        ) {
            final ActionListener<IndexMetadataPublishResponse> responseActionListener;
            responseActionListener = listener;
            sendIndexMetadataState(destination, responseActionListener);
        }

        public void sendIndexMetadataState(DiscoveryNode destination, ActionListener<IndexMetadataPublishResponse> listener) {
            try {
                logger.info("sending new IndexMetadata State IndexMetadata to node: {}", destination.getName());
                final String lastAcceptedIndexMetadataManifestVersion = ((RemotePersistedState) persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE))
                    .getLastAcceptedIndexMetadataManifestVersion();
                final IndexMetadataPublishRequest indexMetadataPublishRequest = new IndexMetadataPublishRequest(
                    discoveryNodes.getLocalNode(),
                    lastAcceptedIndexMetadataManifestVersion
                );
                final Consumer<TransportException> transportExceptionHandler = exp -> {
                    logger.debug(() -> new ParameterizedMessage("failed to send remote cluster state to {}", destination), exp);
                    listener.onFailure(exp);
                };
                final TransportResponseHandler<IndexMetadataPublishResponse> responseHandler = new TransportResponseHandler<>() {

                    @Override
                    public IndexMetadataPublishResponse read(StreamInput in) throws IOException {
                        return new IndexMetadataPublishResponse(in);
                    }

                    @Override
                    public void handleResponse(IndexMetadataPublishResponse response) {
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
                    PUBLISH_INDEX_METADATA_STATE_ACTION_NAME,
                    indexMetadataPublishRequest,
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
