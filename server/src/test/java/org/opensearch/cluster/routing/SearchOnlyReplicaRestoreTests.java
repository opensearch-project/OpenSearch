/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.repositories.IndexId;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;

public class SearchOnlyReplicaRestoreTests extends OpenSearchTestCase {

    public void testSearchOnlyReplicasRestored() {
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .numberOfSearchReplicas(1)
            )
            .build();

        IndexMetadata indexMetadata = metadata.index("test");
        RecoverySource.SnapshotRecoverySource snapshotRecoverySource = new RecoverySource.SnapshotRecoverySource(
            UUIDs.randomBase64UUID(),
            new Snapshot("rep1", new SnapshotId("snp1", UUIDs.randomBase64UUID())),
            Version.CURRENT,
            new IndexId("test", UUIDs.randomBase64UUID(random()))
        );

        RoutingTable routingTable = RoutingTable.builder().addAsNewRestore(indexMetadata, snapshotRecoverySource, new HashSet<>()).build();

        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        IndexShardRoutingTable indexShardRoutingTable = clusterState.routingTable().index("test").shard(0);

        assertEquals(1, clusterState.routingTable().index("test").shards().size());
        assertEquals(3, indexShardRoutingTable.getShards().size());
        assertEquals(1, indexShardRoutingTable.searchOnlyReplicas().size());
    }
}
