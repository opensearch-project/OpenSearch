/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway;

import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.indices.store.ShardAttributes;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.opensearch.test.OpenSearchIntegTestCase.client;
import static org.opensearch.test.OpenSearchIntegTestCase.internalCluster;
import static org.opensearch.test.OpenSearchIntegTestCase.resolveIndex;

public class GatewayRecoveryTestUtils {

    public static DiscoveryNode[] getDiscoveryNodes() throws ExecutionException, InterruptedException {
        final ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.local(false);
        clusterStateRequest.clear().nodes(true).routingTable(true).indices("*");
        ClusterStateResponse clusterStateResponse = client().admin().cluster().state(clusterStateRequest).get();
        final List<DiscoveryNode> nodes = new LinkedList<>(clusterStateResponse.getState().nodes().getDataNodes().values());
        DiscoveryNode[] disNodesArr = new DiscoveryNode[nodes.size()];
        nodes.toArray(disNodesArr);
        return disNodesArr;
    }

    public static Map<ShardId, ShardAttributes> prepareRequestMap(String[] indices, int primaryShardCount) {
        Map<ShardId, ShardAttributes> shardIdShardAttributesMap = new HashMap<>();
        for (String indexName : indices) {
            final Index index = resolveIndex(indexName);
            final String customDataPath = IndexMetadata.INDEX_DATA_PATH_SETTING.get(
                client().admin().indices().prepareGetSettings(indexName).get().getIndexToSettings().get(indexName)
            );
            for (int shardIdNum = 0; shardIdNum < primaryShardCount; shardIdNum++) {
                final ShardId shardId = new ShardId(index, shardIdNum);
                shardIdShardAttributesMap.put(shardId, new ShardAttributes(customDataPath));
            }
        }
        return shardIdShardAttributesMap;
    }

    public static void corruptShard(String nodeName, ShardId shardId) throws IOException, InterruptedException {
        for (Path path : internalCluster().getInstance(NodeEnvironment.class, nodeName).availableShardPaths(shardId)) {
            final Path indexPath = path.resolve(ShardPath.INDEX_FOLDER_NAME);
            if (Files.exists(indexPath)) { // multi data path might only have one path in use
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexPath)) {
                    for (Path item : stream) {
                        if (item.getFileName().toString().startsWith("segments_")) {
                            Files.delete(item);
                        }
                    }
                }
            }
        }
    }
}
