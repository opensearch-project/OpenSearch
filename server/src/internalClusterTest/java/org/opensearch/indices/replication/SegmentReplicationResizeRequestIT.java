/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.admin.indices.shrink.ResizeType;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.Preference;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * This test class verifies Resize Reequests (Shrink, Split, Clone) with segment replication as replication strategy.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
public class SegmentReplicationResizeRequestIT extends SegmentReplicationBaseIT {

    public void testCreateShrinkIndexThrowsExceptionWhenReplicasBehind() throws Exception {

        // create index with -1 as refresh interval as we are blocking segrep and we want to control refreshes.
        prepareCreate("test").setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("index.refresh_interval", -1)
                .put("index.number_of_replicas", 1)
                .put("number_of_shards", 2)
        ).get();

        final Map<String, DiscoveryNode> dataNodes = client().admin().cluster().prepareState().get().getState().nodes().getDataNodes();
        assertTrue("at least 2 nodes but was: " + dataNodes.size(), dataNodes.size() >= 2);
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(new DiscoveryNode[0]);
        // ensure all shards are allocated otherwise the ensure green below might not succeed since we require the merge node
        // if we change the setting too quickly we will end up with one replica unassigned which can't be assigned anymore due
        // to the require._name below.
        ensureGreen();

        // block Segment Replication so that replicas never get the docs from primary
        CountDownLatch latch = new CountDownLatch(1);
        try (final Releasable ignored = blockReplication(List.of(discoveryNodes[0].getName()), latch)) {
            final int docs = 500;
            for (int i = 0; i < docs; i++) {
                client().prepareIndex("test").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", MediaTypeRegistry.JSON).get();
            }

            // block writes on index before performing shrink operation
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(
                    Settings.builder()
                        .put("index.routing.allocation.require._name", discoveryNodes[0].getName())
                        .put("index.blocks.write", true)
                )
                .get();
            ensureGreen();

            // Trigger Shrink operation, as replicas don't have any docs it will throw exception that replicas haven't caught up
            IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> client().admin()
                    .indices()
                    .prepareResizeIndex("test", "target")
                    .setResizeType(ResizeType.SHRINK)
                    .setSettings(
                        Settings.builder()
                            .put("index.number_of_replicas", 1)
                            .putNull("index.blocks.write")
                            .putNull("index.routing.allocation.require._name")
                            .build()
                    )
                    .get()
            );
            assertEquals(
                "Replication still in progress for index [test]. Please wait for replication to complete and retry. "
                    + "Use the _cat/segment_replication/test api to check if the index is up to date (e.g. bytes_behind == 0).",
                exception.getMessage()
            );

        }

    }

    public void testCreateSplitIndexWithSegmentReplicationBlocked() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);

        // create index with -1 as refresh interval as we are blocking segrep and we want to control refreshes.
        prepareCreate("test").setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("index.refresh_interval", -1)
                .put("index.number_of_replicas", 1)
                .put("number_of_shards", 3)
        ).get();

        final Map<String, DiscoveryNode> dataNodes = client().admin().cluster().prepareState().get().getState().nodes().getDataNodes();
        assertTrue("at least 2 nodes but was: " + dataNodes.size(), dataNodes.size() >= 2);
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(new DiscoveryNode[0]);
        // ensure all shards are allocated otherwise the ensure green below might not succeed since we require the merge node
        // if we change the setting too quickly we will end up with one replica unassigned which can't be assigned anymore due
        // to the require._name below.
        ensureGreen();

        CountDownLatch latch = new CountDownLatch(1);

        // block Segment Replication so that replicas never get the docs from primary
        try (final Releasable ignored = blockReplication(List.of(discoveryNodes[0].getName()), latch)) {
            final int docs = 500;
            for (int i = 0; i < docs; i++) {
                client().prepareIndex("test").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", MediaTypeRegistry.JSON).get();
            }
            refresh();
            assertBusy(() -> {
                assertHitCount(
                    client().prepareSearch("test")
                        .setQuery(new TermsQueryBuilder("foo", "bar"))
                        .setPreference(Preference.PRIMARY.type())
                        .get(),
                    docs
                );
            });

            // block writes on index before performing split operation
            client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.blocks.write", true)).get();
            ensureGreen();

            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
                )
                .get();

            // Trigger split operation
            assertAcked(
                client().admin()
                    .indices()
                    .prepareResizeIndex("test", "target")
                    .setResizeType(ResizeType.SPLIT)
                    .setSettings(
                        Settings.builder()
                            .put("index.number_of_replicas", 1)
                            .put("index.number_of_shards", 6)
                            .putNull("index.blocks.write")
                            .build()
                    )
                    .get()
            );
            ensureGreen();

            // verify that all docs are present in new target index
            assertHitCount(
                client().prepareSearch("target")
                    .setQuery(new TermsQueryBuilder("foo", "bar"))
                    .setPreference(Preference.PRIMARY.type())
                    .get(),
                docs
            );
        }

    }

    public void testCloneIndex() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);

        // create index with -1 as refresh interval as we are blocking segrep and we want to control refreshes.
        prepareCreate("test").setSettings(
            Settings.builder().put(indexSettings()).put("index.number_of_replicas", 1).put("number_of_shards", randomIntBetween(1, 5))
        ).get();

        final Map<String, DiscoveryNode> dataNodes = client().admin().cluster().prepareState().get().getState().nodes().getDataNodes();
        assertTrue("at least 2 nodes but was: " + dataNodes.size(), dataNodes.size() >= 2);
        DiscoveryNode[] discoveryNodes = dataNodes.values().toArray(new DiscoveryNode[0]);
        // ensure all shards are allocated otherwise the ensure green below might not succeed since we require the merge node
        // if we change the setting too quickly we will end up with one replica unassigned which can't be assigned anymore due
        // to the require._name below.
        ensureGreen();

        CountDownLatch latch = new CountDownLatch(1);

        // block Segment Replication so that replicas never get the docs from primary
        try (final Releasable ignored = blockReplication(List.of(discoveryNodes[0].getName()), latch)) {
            final int docs = 500;
            for (int i = 0; i < docs; i++) {
                client().prepareIndex("test").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", MediaTypeRegistry.JSON).get();
            }
            refresh();
            assertBusy(() -> {
                assertHitCount(
                    client().prepareSearch("test")
                        .setQuery(new TermsQueryBuilder("foo", "bar"))
                        .setPreference(Preference.PRIMARY.type())
                        .get(),
                    docs
                );
            });

            // block writes on index before performing clone operation
            client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put("index.blocks.write", true)).get();
            ensureGreen();

            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none")
                )
                .get();

            // Trigger split operation
            assertAcked(
                client().admin()
                    .indices()
                    .prepareResizeIndex("test", "target")
                    .setResizeType(ResizeType.CLONE)
                    .setSettings(Settings.builder().put("index.number_of_replicas", 1).putNull("index.blocks.write").build())
                    .get()
            );
            ensureGreen();

            // verify that all docs are present in new target index
            assertHitCount(
                client().prepareSearch("target")
                    .setQuery(new TermsQueryBuilder("foo", "bar"))
                    .setPreference(Preference.PRIMARY.type())
                    .get(),
                docs
            );
        }

    }

}
