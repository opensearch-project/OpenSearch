/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.remotestore.RemoteStoreBaseIntegTestCase.segmentsOnlyNodeAttributes;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Base class for tests of a cluster running with {@code node.attr.remote_store.mode: segments_only}, where segments
 * are uploaded to a remote store but the translog stays on local disk and is replicated node-to-node.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public abstract class SegmentsOnlyBaseIntegTestCase extends OpenSearchIntegTestCase {

    protected static final String SEGMENT_REPOSITORY_NAME = "test-segment-repo";

    protected Path segmentRepoPath;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockTransportService.TestPlugin.class, InternalSettingsPlugin.class))
            .collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        if (startInSegmentsOnlyMode()) {
            settings.put(segmentsOnlyAttributes());
        }
        return settings.build();
    }

    /**
     * Whether nodes start carrying the segments_only attributes. Override to start the cluster in another mode and
     * move it into segments_only partway through, which is what a migration looks like.
     */
    protected boolean startInSegmentsOnlyMode() {
        return true;
    }

    /** A segment repository and deliberately no translog repository, which is what puts a node in segments_only mode. */
    protected Settings segmentsOnlyAttributes() {
        if (segmentRepoPath == null) {
            segmentRepoPath = randomRepoPath().toAbsolutePath();
        }
        return segmentsOnlyNodeAttributes(SEGMENT_REPOSITORY_NAME, segmentRepoPath);
    }

    protected void createSegmentsOnlyIndex(String index, int numReplicas) {
        createSegmentsOnlyIndex(index, numReplicas, 0, Settings.EMPTY);
    }

    protected void createSegmentsOnlyIndex(String index, int numReplicas, Settings extra) {
        createSegmentsOnlyIndex(index, numReplicas, 0, extra);
    }

    protected void createSegmentsOnlyIndex(String index, int numReplicas, int numSearchReplicas, Settings extra) {
        assertAcked(
            prepareCreate(index).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numReplicas)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SEARCH_REPLICAS, numSearchReplicas)
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                    .put(extra)
            )
        );
        ensureGreen(index);
    }

    /** Starts a cluster manager and {@code dataNodes} data nodes, and returns the data node names. */
    protected List<String> startCluster(int dataNodes) {
        internalCluster().startClusterManagerOnlyNode();
        List<String> nodes = internalCluster().startDataOnlyNodes(dataNodes);
        ensureStableCluster(dataNodes + 1);
        return nodes;
    }

    /** The shard 0 copy of {@code index} held by {@code node}. */
    protected IndexShard shardOn(String node, String index) {
        return internalCluster().getInstance(IndicesService.class, node).indexService(resolveIndex(index)).getShard(0);
    }

    /** Asserts the premise every test in this package depends on: remote segments, local translog. */
    protected static void assertSegmentsOnly(IndexShard shard) {
        assertTrue("segments must be remote backed", shard.indexSettings().isRemoteStoreEnabled());
        assertFalse("the translog must stay local", shard.indexSettings().isTranslogDurableInRemoteStore());
    }

    /** Indexes {@code count} documents with ids {@code from} onwards, and returns the next free id. */
    protected int indexDocs(String index, int from, int count) {
        for (int i = from; i < from + count; i++) {
            client().prepareIndex(index).setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }
        return from + count;
    }
}
