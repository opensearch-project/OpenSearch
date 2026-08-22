/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.opensearch.action.search.TransportSearchAction;
import org.opensearch.analytics.planner.IndexResolution;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

/**
 * Resolves {@link ShardExecutionTarget}s for a DATA_NODE scan stage.
 * Reads the pre-resolved {@link IndexResolution} carried on the fragment's
 * {@link OpenSearchTableScan} node, then resolves shard targets lazily when
 * the Scheduler calls {@link #resolve}.
 *
 * <p>Shard routing gives both the node and the shardId in one pass —
 * these are coupled and cannot be separated, hence a dedicated resolver
 * rather than {@link ComposableTargetResolver}.
 *
 * @opensearch.internal
 */
public class ShardTargetResolver extends TargetResolver {

    private final IndexResolution carriedResolution;
    private final ClusterService clusterService;

    /**
     * Reads the carried {@link IndexResolution} from the fragment's {@link OpenSearchTableScan}.
     * Fails if absent — a silent fallback would re-resolve with different {@code IndicesOptions}.
     */
    public ShardTargetResolver(RelNode fragment, ClusterService clusterService) {
        // Plural lookup: the scan may sit on a non-first join input a first-input-only walk cannot reach.
        // First is safe: the only multi-scan shape is the collocated single-shard join, all scans same table and shard.
        List<OpenSearchTableScan> scans = RelNodeUtils.findNodes(fragment, OpenSearchTableScan.class);
        if (scans.isEmpty()) {
            throw new IllegalArgumentException("ShardTargetResolver: no OpenSearchTableScan found in fragment");
        }
        OpenSearchTableScan scan = scans.getFirst();
        IndexResolution resolution = scan.getCarriedResolution();
        if (resolution == null) {
            throw new IllegalStateException(
                "ShardTargetResolver: fragment's OpenSearchTableScan ["
                    + scan.getTable().getQualifiedName()
                    + "] does not carry a pre-resolved IndexResolution. "
                    + "All scan nodes must carry their resolution from planning to avoid re-resolution with different IndicesOptions."
            );
        }
        this.carriedResolution = resolution;
        this.clusterService = clusterService;
    }

    @Override
    public List<ExecutionTarget> resolve(ClusterState clusterState, @Nullable Object childManifest) {
        // Reuse the planner's resolution — re-resolving could use different IndicesOptions.
        IndexResolution resolution = carriedResolution;
        String[] concreteNames = resolution.concreteIndexNames().toArray(new String[0]);
        GroupShardsIterator<ShardIterator> shardIterators = clusterService.operationRouting()
            .searchShards(clusterState, concreteNames, null, null);
        // Same operator-facing ceiling vanilla search enforces, read live so a dynamic update takes
        // effect on the next query. Unlimited by default: the can-match pre-filter phase and the
        // per-node dispatch throttle are what bound fan-out in normal operation, and this stays a
        // valve for operators who want a hard stop.
        long shardCountLimit = clusterService.getClusterSettings().get(TransportSearchAction.SHARD_COUNT_LIMIT_SETTING);
        int shardCount = shardIterators.size();
        if (shardCount > shardCountLimit) {
            String sourceType = describeIndexSource(resolution.requestedName(), clusterState);
            throw new IllegalArgumentException(
                "Query via "
                    + sourceType
                    + " targets ["
                    + shardCount
                    + "] shards which exceeds the limit of ["
                    + shardCountLimit
                    + "] set by ["
                    + TransportSearchAction.SHARD_COUNT_LIMIT_SETTING.getKey()
                    + "]. This limit exists because querying many shards at the same time can make the job of the "
                    + "coordinating node very CPU and/or memory intensive. Query fewer indices, or raise the limit."
            );
        }
        List<ExecutionTarget> targets = new ArrayList<>();
        int ordinal = 0;
        for (ShardIterator shardIt : shardIterators) {
            ShardRouting shard = shardIt.nextOrNull();
            if (shard != null) {
                DiscoveryNode node = clusterState.nodes().get(shard.currentNodeId());
                if (node != null) {
                    // Pass the remaining iterator + cluster state to the target so dispatch
                    // failure can fall over to a replica copy via ShardExecutionTarget.nextCopy.
                    targets.add(new ShardExecutionTarget(node, shard.shardId(), ordinal++, shardIt, clusterState));
                }
            }
        }
        return targets;
    }

    private static String describeIndexSource(String name, ClusterState clusterState) {
        SortedMap<String, IndexAbstraction> lookup = clusterState.metadata().getIndicesLookup();
        IndexAbstraction abstraction = lookup != null ? lookup.get(name) : null;
        if (abstraction != null) {
            return switch (abstraction.getType()) {
                case ALIAS -> "alias [" + name + "]";
                case DATA_STREAM -> "data stream [" + name + "]";
                case CONCRETE_INDEX -> "index [" + name + "]";
            };
        }
        return "index pattern [" + name + "]";
    }
}
