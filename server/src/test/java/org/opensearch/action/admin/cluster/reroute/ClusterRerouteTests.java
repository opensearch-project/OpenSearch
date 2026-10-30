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

package org.opensearch.action.admin.cluster.reroute;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.EmptyClusterInfoService;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.FailedShard;
import org.opensearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.opensearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.opensearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.opensearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.fielddomain.IndexFieldDomainMetadata;
import org.opensearch.snapshots.EmptySnapshotsInfoService;
import org.opensearch.test.gateway.TestGatewayAllocator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.opensearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ClusterRerouteTests extends OpenSearchAllocationTestCase {

    public void testRerouteResponsesExcludePrivateIndexMetadata() throws IOException {
        for (boolean dryRun : List.of(false, true)) {
            for (boolean ackTimeout : List.of(false, true)) {
                AllocationService allocationService = createAllocationService(Settings.EMPTY);
                ClusterState initial = createInitialClusterState(allocationService);
                Map<String, String> privateData = Map.of("field", "private-value");
                Map<String, String> publicData = Map.of("key", "public-value");
                IndexMetadata indexMetadata = IndexMetadata.builder(initial.metadata().index("idx"))
                    .putCustom(IndexFieldDomainMetadata.CUSTOM_KEY, privateData)
                    .putCustom("plugin_metadata", publicData)
                    .build();
                ClusterState state = ClusterState.builder(initial)
                    .metadata(Metadata.builder(initial.metadata()).put(indexMetadata, false))
                    .build();
                AtomicReference<ClusterRerouteResponse> responseRef = new AtomicReference<>();
                TransportClusterRerouteAction.ClusterRerouteResponseAckedClusterStateUpdateTask task =
                    new TransportClusterRerouteAction.ClusterRerouteResponseAckedClusterStateUpdateTask(
                        logger,
                        allocationService,
                        new ClusterRerouteRequest().dryRun(dryRun),
                        ActionListener.wrap(responseRef::set, e -> {
                            throw new AssertionError(e);
                        })
                    );
                ClusterState applied = task.execute(state);
                if (dryRun) {
                    assertSame(state, applied);
                }
                if (ackTimeout) {
                    task.onAckTimeout();
                } else {
                    task.onAllNodesAcked(null);
                }
                ClusterRerouteResponse response = responseRef.get();
                assertEquals(ackTimeout == false, response.isAcknowledged());
                assertNull(response.getState().metadata().index("idx").getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY));
                assertEquals(publicData, response.getState().metadata().index("idx").getCustomData("plugin_metadata"));
                assertEquals(indexMetadata.getVersion(), response.getState().metadata().index("idx").getVersion());
                assertEquals(privateData, state.metadata().index("idx").getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY));
                assertEquals(privateData, applied.metadata().index("idx").getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY));
                try (BytesStreamOutput out = new BytesStreamOutput()) {
                    response.writeTo(out);
                    try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())) {
                        assertEquals(response.isAcknowledged(), in.readBoolean());
                        ClusterState wireState = ClusterState.readFrom(in, null);
                        assertNull(wireState.metadata().index("idx").getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY));
                        assertEquals(publicData, wireState.metadata().index("idx").getCustomData("plugin_metadata"));
                    }
                    try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())) {
                        ClusterRerouteResponse restored = new ClusterRerouteResponse(in);
                        assertNull(restored.getState().metadata().index("idx").getCustomData(IndexFieldDomainMetadata.CUSTOM_KEY));
                        assertEquals(publicData, restored.getState().metadata().index("idx").getCustomData("plugin_metadata"));
                    }
                }
            }
        }
    }

    public void testSerializeRequest() throws IOException {
        ClusterRerouteRequest req = new ClusterRerouteRequest();
        req.setRetryFailed(randomBoolean());
        req.dryRun(randomBoolean());
        req.explain(randomBoolean());
        req.add(new AllocateEmptyPrimaryAllocationCommand("foo", 1, "bar", randomBoolean()));
        req.timeout(TimeValue.timeValueMillis(randomIntBetween(0, 100)));
        BytesStreamOutput out = new BytesStreamOutput();
        req.writeTo(out);
        BytesReference bytes = out.bytes();
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(NetworkModule.getNamedWriteables());
        StreamInput wrap = new NamedWriteableAwareStreamInput(bytes.streamInput(), namedWriteableRegistry);
        ClusterRerouteRequest deserializedReq = new ClusterRerouteRequest(wrap);

        assertEquals(req.isRetryFailed(), deserializedReq.isRetryFailed());
        assertEquals(req.dryRun(), deserializedReq.dryRun());
        assertEquals(req.explain(), deserializedReq.explain());
        assertEquals(req.timeout(), deserializedReq.timeout());
        assertEquals(1, deserializedReq.getCommands().commands().size()); // allocation commands have their own tests
        assertEquals(req.getCommands().commands().size(), deserializedReq.getCommands().commands().size());
    }

    public void testClusterStateUpdateTask() {
        AllocationService allocationService = new AllocationService(
            new AllocationDeciders(Collections.singleton(new MaxRetryAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );
        ClusterState clusterState = createInitialClusterState(allocationService);
        ClusterRerouteRequest req = new ClusterRerouteRequest();
        req.dryRun(true);
        AtomicReference<ClusterRerouteResponse> responseRef = new AtomicReference<>();
        ActionListener<ClusterRerouteResponse> responseActionListener = new ActionListener<ClusterRerouteResponse>() {
            @Override
            public void onResponse(ClusterRerouteResponse clusterRerouteResponse) {
                responseRef.set(clusterRerouteResponse);
            }

            @Override
            public void onFailure(Exception e) {

            }
        };
        TransportClusterRerouteAction.ClusterRerouteResponseAckedClusterStateUpdateTask task =
            new TransportClusterRerouteAction.ClusterRerouteResponseAckedClusterStateUpdateTask(
                logger,
                allocationService,
                req,
                responseActionListener
            );
        ClusterState execute = task.execute(clusterState);
        assertSame(execute, clusterState); // dry-run
        task.onAllNodesAcked(null);
        assertNotSame(responseRef.get().getState(), execute);

        req.dryRun(false);// now we allocate

        final int retries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        // now fail it N-1 times
        for (int i = 0; i < retries; i++) {
            ClusterState newState = task.execute(clusterState);
            assertNotSame(newState, clusterState); // dry-run=false
            clusterState = newState;
            RoutingTable routingTable = clusterState.routingTable();
            assertEquals(routingTable.index("idx").shards().size(), 1);
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), i);
            List<FailedShard> failedShards = Collections.singletonList(
                new FailedShard(
                    routingTable.index("idx").shard(0).shards().get(0),
                    "boom" + i,
                    new UnsupportedOperationException(),
                    randomBoolean()
                )
            );
            newState = allocationService.applyFailedShards(clusterState, failedShards);
            assertThat(newState, not(equalTo(clusterState)));
            clusterState = newState;
            routingTable = clusterState.routingTable();
            assertEquals(routingTable.index("idx").shards().size(), 1);
            if (i == retries - 1) {
                assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), UNASSIGNED);
            } else {
                assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
            }
            assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), i + 1);
        }

        // without retry_failed we won't allocate that shard
        ClusterState newState = task.execute(clusterState);
        assertNotSame(newState, clusterState); // dry-run=false
        task.onAllNodesAcked(null);
        assertSame(responseRef.get().getState(), newState);
        RoutingTable routingTable = clusterState.routingTable();
        assertEquals(routingTable.index("idx").shards().size(), 1);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), UNASSIGNED);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations(), retries);

        req.setRetryFailed(true); // now we manually retry and get the shard back into initializing
        newState = task.execute(clusterState);
        assertNotSame(newState, clusterState); // dry-run=false
        clusterState = newState;
        routingTable = clusterState.routingTable();
        assertEquals(1, routingTable.index("idx").shards().size());
        assertEquals(INITIALIZING, routingTable.index("idx").shard(0).shards().get(0).state());
        assertEquals(0, routingTable.index("idx").shard(0).shards().get(0).unassignedInfo().getNumFailedAllocations());
    }

    private ClusterState createInitialClusterState(AllocationService service) {
        Metadata.Builder metaBuilder = Metadata.builder();
        metaBuilder.put(IndexMetadata.builder("idx").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0));
        Metadata metadata = metaBuilder.build();
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        routingTableBuilder.addAsNew(metadata.index("idx"));

        RoutingTable routingTable = routingTableBuilder.build();
        ClusterState clusterState = ClusterState.builder(org.opensearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        RoutingTable prevRoutingTable = routingTable;
        routingTable = service.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertEquals(prevRoutingTable.index("idx").shards().size(), 1);
        assertEquals(prevRoutingTable.index("idx").shard(0).shards().get(0).state(), UNASSIGNED);

        assertEquals(routingTable.index("idx").shards().size(), 1);
        assertEquals(routingTable.index("idx").shard(0).shards().get(0).state(), INITIALIZING);
        return clusterState;
    }
}
