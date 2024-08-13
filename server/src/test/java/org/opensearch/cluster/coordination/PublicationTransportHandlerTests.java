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

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.node.Node;
import org.opensearch.telemetry.tracing.noop.NoopTracer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.transport.CapturingTransport;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;

import org.mockito.Mockito;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class PublicationTransportHandlerTests extends OpenSearchTestCase {

    private static final long TERM = 5;
    private static final long VERSION = 5;
    private static final String CLUSTER_NAME = "test-cluster";
    private static final String CLUSTER_UUID = "test-cluster-UUID";
    private static final String MANIFEST_FILE = "path/to/manifest";
    private static final String LOCAL_NODE_ID = "localNode";

    private DeterministicTaskQueue deterministicTaskQueue;
    private TransportService transportService;
    private DiscoveryNode localNode;
    private DiscoveryNode secondNode;

    @Before
    public void setup() {
        deterministicTaskQueue = new DeterministicTaskQueue(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "test").build(),
            random()
        );
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        localNode = new DiscoveryNode(LOCAL_NODE_ID, buildNewFakeTransportAddress(), Version.CURRENT);
        secondNode = new DiscoveryNode("secondNode", buildNewFakeTransportAddress(), Version.CURRENT);
        transportService = new CapturingTransport().createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            x -> localNode,
            clusterSettings,
            Collections.emptySet(),
            NoopTracer.INSTANCE
        );
    }

    public void testDiffSerializationFailure() {
        final PublicationTransportHandler handler = getPublicationTransportHandler(p -> null, null);

        final DiscoveryNode otherNode = new DiscoveryNode("otherNode", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState clusterState = CoordinationStateTests.clusterState(
            2L,
            1L,
            DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId(localNode.getId()).build(),
            VotingConfiguration.EMPTY_CONFIG,
            VotingConfiguration.EMPTY_CONFIG,
            0L
        );

        final ClusterState unserializableClusterState = new ClusterState(clusterState.version(), clusterState.stateUUID(), clusterState) {
            @Override
            public Diff<ClusterState> diff(ClusterState previousState) {
                return new Diff<ClusterState>() {
                    @Override
                    public ClusterState apply(ClusterState part) {
                        fail("this diff shouldn't be applied");
                        return part;
                    }

                    @Override
                    public void writeTo(StreamOutput out) throws IOException {
                        throw new IOException("Simulated failure of diff serialization");
                    }
                };
            }
        };

        OpenSearchException e = expectThrows(
            OpenSearchException.class,
            () -> handler.newPublicationContext(new ClusterChangedEvent("test", unserializableClusterState, clusterState), false, null)
        );
        assertNotNull(e.getCause());
        assertThat(e.getCause(), instanceOf(IOException.class));
        assertThat(e.getCause().getMessage(), containsString("Simulated failure of diff serialization"));
    }

    public void testHandleIncomingRemotePublishRequestWhenNoCurrentPublishRequest() {
        RemoteClusterStateService remoteClusterStateService = mock(RemoteClusterStateService.class);

        PublishWithJoinResponse expectedPublishResponse = new PublishWithJoinResponse(new PublishResponse(TERM, VERSION), Optional.empty());
        Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest = p -> expectedPublishResponse;
        final PublicationTransportHandler handler = getPublicationTransportHandler(handlePublishRequest, remoteClusterStateService);
        RemotePublishRequest remotePublishRequest = new RemotePublishRequest(
            localNode,
            TERM,
            VERSION,
            CLUSTER_NAME,
            CLUSTER_UUID,
            MANIFEST_FILE
        );

        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> handler.handleIncomingRemotePublishRequest(remotePublishRequest)
        );
        assertThat(e.getMessage(), containsString("publication to self failed"));
        Mockito.verifyNoInteractions(remoteClusterStateService);
    }

    public void testHandleIncomingRemotePublishRequestWhenTermMismatch() {
        RemoteClusterStateService remoteClusterStateService = mock(RemoteClusterStateService.class);

        PublishWithJoinResponse expectedPublishResponse = new PublishWithJoinResponse(new PublishResponse(TERM, VERSION), Optional.empty());
        Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest = p -> expectedPublishResponse;
        final PublicationTransportHandler handler = getPublicationTransportHandler(handlePublishRequest, remoteClusterStateService);
        RemotePublishRequest remotePublishRequest = new RemotePublishRequest(
            localNode,
            TERM,
            VERSION,
            CLUSTER_NAME,
            CLUSTER_UUID,
            MANIFEST_FILE
        );
        ClusterState clusterState = buildClusterState(6L, VERSION);
        PublishRequest publishRequest = new PublishRequest(clusterState);
        handler.setCurrentPublishRequestToSelf(publishRequest);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> handler.handleIncomingRemotePublishRequest(remotePublishRequest)
        );
        assertThat(e.getMessage(), containsString("publication to self failed"));
        Mockito.verifyNoInteractions(remoteClusterStateService);
    }

    public void testHandleIncomingRemotePublishRequestWhenVersionMismatch() {
        RemoteClusterStateService remoteClusterStateService = mock(RemoteClusterStateService.class);

        PublishWithJoinResponse expectedPublishResponse = new PublishWithJoinResponse(new PublishResponse(TERM, VERSION), Optional.empty());
        Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest = p -> expectedPublishResponse;
        final PublicationTransportHandler handler = getPublicationTransportHandler(handlePublishRequest, remoteClusterStateService);
        RemotePublishRequest remotePublishRequest = new RemotePublishRequest(
            localNode,
            TERM,
            VERSION,
            CLUSTER_NAME,
            CLUSTER_UUID,
            MANIFEST_FILE
        );
        ClusterState clusterState = buildClusterState(TERM, 11L);
        PublishRequest publishRequest = new PublishRequest(clusterState);
        handler.setCurrentPublishRequestToSelf(publishRequest);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> handler.handleIncomingRemotePublishRequest(remotePublishRequest)
        );
        assertThat(e.getMessage(), containsString("publication to self failed"));
        Mockito.verifyNoInteractions(remoteClusterStateService);
    }

    public void testHandleIncomingRemotePublishRequestForLocalNode() throws IOException {
        RemoteClusterStateService remoteClusterStateService = mock(RemoteClusterStateService.class);

        PublishWithJoinResponse expectedPublishResponse = new PublishWithJoinResponse(new PublishResponse(TERM, VERSION), Optional.empty());
        Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest = p -> expectedPublishResponse;
        final PublicationTransportHandler handler = getPublicationTransportHandler(handlePublishRequest, remoteClusterStateService);
        RemotePublishRequest remotePublishRequest = new RemotePublishRequest(
            localNode,
            TERM,
            VERSION,
            CLUSTER_NAME,
            CLUSTER_UUID,
            MANIFEST_FILE
        );
        ClusterState clusterState = buildClusterState(TERM, VERSION);
        PublishRequest publishRequest = new PublishRequest(clusterState);
        handler.setCurrentPublishRequestToSelf(publishRequest);
        PublishWithJoinResponse publishWithJoinResponse = handler.handleIncomingRemotePublishRequest(remotePublishRequest);
        assertThat(publishWithJoinResponse, is(expectedPublishResponse));
        Mockito.verifyNoInteractions(remoteClusterStateService);
    }

    public void testHandleIncomingRemotePublishRequestWhenManifestNotFound() throws IOException {
        RemoteClusterStateService remoteClusterStateService = mock(RemoteClusterStateService.class);

        PublishWithJoinResponse expectedPublishResponse = new PublishWithJoinResponse(new PublishResponse(TERM, VERSION), Optional.empty());
        Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest = p -> expectedPublishResponse;
        final PublicationTransportHandler handler = getPublicationTransportHandler(handlePublishRequest, remoteClusterStateService);
        RemotePublishRequest remotePublishRequest = new RemotePublishRequest(
            secondNode,
            TERM,
            VERSION,
            CLUSTER_NAME,
            CLUSTER_UUID,
            MANIFEST_FILE
        );
        when(remoteClusterStateService.getClusterMetadataManifestByFileName(CLUSTER_UUID, MANIFEST_FILE)).thenReturn(null);
        ClusterState clusterState = buildClusterState(TERM, VERSION);
        PublishRequest publishRequest = new PublishRequest(clusterState);
        handler.setCurrentPublishRequestToSelf(publishRequest);
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> handler.handleIncomingRemotePublishRequest(remotePublishRequest)
        );
        assertThat(e.getMessage(), containsString("Publication failed as manifest was not found for"));
        Mockito.verify(remoteClusterStateService, times(1)).getClusterMetadataManifestByFileName(Mockito.any(), Mockito.any());
    }

    public void testHandleIncomingRemotePublishRequestWhenNoLastSeenState() throws IOException {
        RemoteClusterStateService remoteClusterStateService = mock(RemoteClusterStateService.class);

        PublishWithJoinResponse expectedPublishResponse = new PublishWithJoinResponse(new PublishResponse(TERM, VERSION), Optional.empty());
        Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest = p -> expectedPublishResponse;
        final PublicationTransportHandler handler = getPublicationTransportHandler(handlePublishRequest, remoteClusterStateService);
        RemotePublishRequest remotePublishRequest = new RemotePublishRequest(
            secondNode,
            TERM,
            VERSION,
            CLUSTER_NAME,
            CLUSTER_UUID,
            MANIFEST_FILE
        );
        ClusterMetadataManifest manifest = ClusterMetadataManifest.builder().clusterTerm(TERM).stateVersion(VERSION).build();
        when(remoteClusterStateService.getClusterMetadataManifestByFileName(CLUSTER_UUID, MANIFEST_FILE)).thenReturn(manifest);
        when(remoteClusterStateService.getClusterStateForManifest(CLUSTER_NAME, manifest, LOCAL_NODE_ID, true)).thenReturn(
            buildClusterState(TERM, VERSION)
        );
        ClusterState clusterState = buildClusterState(TERM, VERSION);
        PublishRequest publishRequest = new PublishRequest(clusterState);
        handler.setCurrentPublishRequestToSelf(publishRequest);
        PublishWithJoinResponse publishWithJoinResponse = handler.handleIncomingRemotePublishRequest(remotePublishRequest);
        assertThat(publishWithJoinResponse, is(expectedPublishResponse));
        Mockito.verify(remoteClusterStateService, times(1)).getClusterMetadataManifestByFileName(Mockito.any(), Mockito.any());
    }

    public void testHandleIncomingRemotePublicationRequest_WhenApplyFullStateSettingEnabled() throws IOException {
        RemoteClusterStateService remoteClusterStateService = mock(RemoteClusterStateService.class);

        PublishWithJoinResponse expectedPublishResponse = new PublishWithJoinResponse(new PublishResponse(TERM, VERSION), Optional.empty());
        Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest = p -> expectedPublishResponse;
        final PublicationTransportHandler handler = getPublicationTransportHandler(handlePublishRequest, remoteClusterStateService);
        RemotePublishRequest remotePublishRequest = new RemotePublishRequest(
            secondNode,
            TERM,
            VERSION,
            CLUSTER_NAME,
            CLUSTER_UUID,
            MANIFEST_FILE
        );
        ClusterMetadataManifest manifest = ClusterMetadataManifest.builder().clusterTerm(TERM).stateVersion(VERSION).build();
        when(remoteClusterStateService.getRemotePublicationApplyFullState()).thenReturn(true);
        when(remoteClusterStateService.getClusterMetadataManifestByFileName(CLUSTER_UUID, MANIFEST_FILE)).thenReturn(manifest);
        when(remoteClusterStateService.getClusterStateForManifest(CLUSTER_NAME, manifest, LOCAL_NODE_ID, true)).thenReturn(
            buildClusterState(TERM, VERSION)
        );
        ClusterState clusterState = buildClusterState(TERM, VERSION);
        PublishRequest publishRequest = new PublishRequest(clusterState);
        handler.setCurrentPublishRequestToSelf(publishRequest);
        PublishWithJoinResponse publishWithJoinResponse = handler.handleIncomingRemotePublishRequest(remotePublishRequest);
        assertThat(publishWithJoinResponse, is(expectedPublishResponse));
        verify(remoteClusterStateService, times(1)).getClusterMetadataManifestByFileName(Mockito.any(), Mockito.any());
        verify(remoteClusterStateService, times(1)).getClusterStateForManifest(CLUSTER_NAME, manifest, LOCAL_NODE_ID, true);
        verify(remoteClusterStateService, times(1)).getRemotePublicationApplyFullState();
        verifyNoMoreInteractions(remoteClusterStateService);
    }

    private PublicationTransportHandler getPublicationTransportHandler(
        Function<PublishRequest, PublishWithJoinResponse> handlePublishRequest,
        RemoteClusterStateService remoteClusterStateService
    ) {
        final PublicationTransportHandler handler = new PublicationTransportHandler(
            transportService,
            writableRegistry(),
            handlePublishRequest,
            (pu, l) -> {},
            remoteClusterStateService
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        return handler;
    }

    private ClusterState buildClusterState(long term, long version) {
        CoordinationMetadata.Builder coordMetadataBuilder = CoordinationMetadata.builder().term(term);
        Metadata newMetadata = Metadata.builder().coordinationMetadata(coordMetadataBuilder.build()).build();
        DiscoveryNodes nodes = DiscoveryNodes.builder().add(localNode).add(secondNode).localNodeId(LOCAL_NODE_ID).build();
        return ClusterState.builder(ClusterState.EMPTY_STATE).version(version).metadata(newMetadata).nodes(nodes).build();
    }
}
