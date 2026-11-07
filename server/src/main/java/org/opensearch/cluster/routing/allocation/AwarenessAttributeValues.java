/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeFilters;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.opensearch.common.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.opensearch.cluster.node.DiscoveryNodeFilters.OpType.OR;

/**
 * Memoizes the distinct awareness attribute values that
 * {@link org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider} balances shard copies
 * across, with the cluster-level allocation exclude filters
 * ({@code cluster.routing.allocation.exclude.*}) applied. An attribute value is dropped from the balance only
 * when every node carrying it is excluded; if a single non-excluded node remains, the value still counts. This
 * lets a fully-drained value (for example a zone being replaced) leave the balance so that its shards can
 * consolidate onto the remaining values. Index-level allocation filters
 * ({@code index.routing.allocation.include/exclude/require.*}) are deliberately not considered: they vary per index
 * and do not represent an operator draining an attribute value out of the cluster.
 * <p>
 * The decider is consulted once per (shard, node) pair, so recomputing the value set on every decision would
 * cost {@code O(shards * nodes^2)} node visits per allocation round. Memoizing reduces that to one
 * {@code O(nodes)} pass per attribute per node type.
 * <p>
 * Both inputs are fixed for the lifetime of a single allocation round: the node attributes come from the
 * {@link org.opensearch.cluster.routing.RoutingNodes} of one cluster state, and the exclude filters are
 * derived from that same cluster state's settings via {@link RoutingAllocation#clusterSettings()}. A new
 * instance is therefore created per {@link RoutingAllocation} and discarded with it, so there is nothing to
 * invalidate. No synchronization is needed because an allocation round is single threaded, as the neighbouring
 * {@code RoutingAllocation#ignoredShardToNodes} scratch map already assumes.
 * <p>
 * Reached only through {@link RoutingAllocation#awarenessAttributeValues(String, boolean)}; kept package private so
 * that this scratch structure stays out of the public API that {@link RoutingAllocation} exposes.
 *
 * @opensearch.internal
 */
final class AwarenessAttributeValues {

    private final RoutingAllocation allocation;
    private final Map<String, Set<String>> dataNodeValues = new HashMap<>();
    private final Map<String, Set<String>> searchNodeValues = new HashMap<>();
    @Nullable
    private final DiscoveryNodeFilters excludeFilters;

    /**
     * Builds the cluster-level allocation exclude filters once for this allocation round.
     * {@link RoutingAllocation#clusterSettings()} is derived from the cluster state, so it cannot change while
     * this round is in flight.
     */
    AwarenessAttributeValues(RoutingAllocation allocation) {
        this.allocation = allocation;
        this.excludeFilters = DiscoveryNodeFilters.trimTier(
            DiscoveryNodeFilters.buildOrUpdateFromKeyValue(
                null,
                OR,
                FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING.getAsMap(allocation.clusterSettings())
            )
        );
    }

    /**
     * Returns the distinct, non-excluded values of the given awareness attribute across the nodes eligible to
     * hold the shard. Nodes are partitioned by type: search-only shards look at search nodes, every other
     * shard looks at data nodes.
     *
     * @param attributeName the awareness attribute to collect values for
     * @param searchOnly    whether the shard is search-only, selecting search nodes rather than data nodes
     * @return the distinct, non-excluded values for the attribute
     */
    Set<String> get(String attributeName, boolean searchOnly) {
        final Map<String, Set<String>> memo = searchOnly ? searchNodeValues : dataNodeValues;
        return memo.computeIfAbsent(attributeName, attribute -> compute(attribute, searchOnly));
    }

    private Set<String> compute(String attributeName, boolean searchOnly) {
        final Set<String> values = new HashSet<>();
        for (final RoutingNode routingNode : allocation.routingNodes()) {
            final DiscoveryNode node = routingNode.node();
            // only consider nodes of the same type as the shard
            if (node.isSearchNode() != searchOnly) {
                continue;
            }
            // skip nodes matched by the cluster exclude filter (_ip, _id, _name, _host, or node attributes)
            if (excludeFilters != null && excludeFilters.match(node)) {
                continue;
            }
            final String value = node.getAttributes().get(attributeName);
            if (value != null) {
                values.add(value);
            }
        }
        return values;
    }
}
