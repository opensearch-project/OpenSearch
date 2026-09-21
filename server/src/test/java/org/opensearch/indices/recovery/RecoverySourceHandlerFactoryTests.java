/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RecoverySourceHandlerFactoryTests extends OpenSearchTestCase {

    private static DiscoveryNode node(String id, Map<String, String> attributes) {
        return new DiscoveryNode(id, id, buildNewFakeTransportAddress(), attributes, DiscoveryNodeRole.BUILT_IN_ROLES, Version.CURRENT);
    }

    private RecoverySourceHandler handlerForReplicaRecoveryTo(Map<String, String> nodeAttributes) {
        IndexShard shard = mock(IndexShard.class);
        when(shard.getThreadPool()).thenReturn(mock(ThreadPool.class));

        StartRecoveryRequest request = new StartRecoveryRequest(
            new ShardId(new Index("test_index", "_na_"), 0),
            "target-allocation-id",
            node("source-node", nodeAttributes),
            node("target-node", nodeAttributes),
            Store.MetadataSnapshot.EMPTY,
            false,
            1L,
            SequenceNumbers.UNASSIGNED_SEQ_NO
        );

        RecoverySettings recoverySettings = new RecoverySettings(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        return RecoverySourceHandlerFactory.create(shard, mock(RecoveryTargetHandler.class), request, recoverySettings);
    }

    private static Map<String, String> segmentsOnlyAttributes() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, "segment-repo");
        return attributes;
    }

    /**
     * A replica recovering onto a segments-only node has no remote translog to replay history from, so recovery must
     * send operations over the wire.
     */
    public void testSegmentsOnlyReplicaRecoverySendsOperations() {
        assertThat(handlerForReplicaRecoveryTo(segmentsOnlyAttributes()), instanceOf(LocalStorePeerRecoverySourceHandler.class));
    }

    /**
     * Adding a cluster state repository does not give the target a remote translog, so recovery must still send
     * operations. RecoverySourceHandlerFactory keys off DiscoveryNode#isRemoteTranslogStoreNode, so it keeps picking
     * the local store handler instead of the remote store handler whose phase 2 sends nothing.
     */
    public void testSegmentsOnlyWithClusterStateRepoReplicaRecoverySendsOperations() {
        Map<String, String> attributes = segmentsOnlyAttributes();
        attributes.put(RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, "state-repo");
        assertThat(handlerForReplicaRecoveryTo(attributes), instanceOf(LocalStorePeerRecoverySourceHandler.class));
    }
}
