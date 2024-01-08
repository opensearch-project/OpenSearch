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

package org.opensearch.gateway;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.PersistedStateRegistry;
import org.opensearch.cluster.metadata.Manifest;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.MetadataIndexUpgradeService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.index.recovery.RemoteStoreRestoreService;
import org.opensearch.plugins.MetadataUpgrader;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link GatewayMetaState} constructor accepts a lot of arguments.
 * It's not always easy / convenient to construct these dependencies.
 * This class constructor takes far fewer dependencies and constructs usable {@link GatewayMetaState} with 2 restrictions:
 * no metadata upgrade will be performed and no cluster state updaters will be run. This is sufficient for most of the tests.
 */
public class MockGatewayMetaState extends GatewayMetaState {
    private final DiscoveryNode localNode;
    private final BigArrays bigArrays;
    private final RemoteClusterStateService remoteClusterStateService;
    private final RemoteStoreRestoreService remoteStoreRestoreService;
    private boolean prepareFullState = false;

    public MockGatewayMetaState(DiscoveryNode localNode, BigArrays bigArrays, boolean prepareFullState) {
        this(localNode, bigArrays);
        this.prepareFullState = prepareFullState;
    }

    public MockGatewayMetaState(DiscoveryNode localNode, BigArrays bigArrays) {
        this.localNode = localNode;
        this.bigArrays = bigArrays;
        this.remoteClusterStateService = mock(RemoteClusterStateService.class);
        this.remoteStoreRestoreService = mock(RemoteStoreRestoreService.class);
    }

    public MockGatewayMetaState(
        DiscoveryNode localNode,
        BigArrays bigArrays,
        RemoteClusterStateService remoteClusterStateService,
        RemoteStoreRestoreService remoteStoreRestoreService
    ) {
        this.localNode = localNode;
        this.bigArrays = bigArrays;
        this.remoteClusterStateService = remoteClusterStateService;
        this.remoteStoreRestoreService = remoteStoreRestoreService;
    }

    @Override
    Metadata upgradeMetadataForNode(
        Metadata metadata,
        MetadataIndexUpgradeService metadataIndexUpgradeService,
        MetadataUpgrader metadataUpgrader
    ) {
        // Metadata upgrade is tested in GatewayMetaStateTests, we override this method to NOP to make mocking easier
        return metadata;
    }

    @Override
    ClusterState prepareInitialClusterState(TransportService transportService, ClusterService clusterService, ClusterState clusterState) {
        if (prepareFullState) {
            return super.prepareInitialClusterState(transportService, clusterService, clusterState);
        } else {
            // Just set localNode here, not to mess with ClusterService and IndicesService mocking
            return ClusterStateUpdaters.setLocalNode(clusterState, localNode);
        }
    }

    @Override
    public void close() throws IOException {
        super.close();
    }

    public void start(
        Settings settings,
        NodeEnvironment nodeEnvironment,
        NamedXContentRegistry xContentRegistry,
        PersistedStateRegistry persistedStateRegistry
    ) {
        start(settings, nodeEnvironment, xContentRegistry, persistedStateRegistry, false);
    }

    public void start(
        Settings settings,
        NodeEnvironment nodeEnvironment,
        NamedXContentRegistry xContentRegistry,
        PersistedStateRegistry persistedStateRegistry,
        boolean prepareFullState
    ) {
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getThreadPool()).thenReturn(mock(ThreadPool.class));
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        final MetaStateService metaStateService = mock(MetaStateService.class);
        try {
            when(metaStateService.loadFullState()).thenReturn(new Tuple<>(Manifest.empty(), Metadata.builder().build()));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        this.prepareFullState = prepareFullState;
        start(
            settings,
            transportService,
            clusterService,
            metaStateService,
            null,
            null,
            new PersistedClusterStateService(
                nodeEnvironment,
                xContentRegistry,
                bigArrays,
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                () -> 0L
            ),
            remoteClusterStateService,
            persistedStateRegistry,
            remoteStoreRestoreService
        );
    }
}
