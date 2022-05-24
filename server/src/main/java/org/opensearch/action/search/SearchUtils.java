/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Strings;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.Transport;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Helper class for common search functions
 */
public class SearchUtils {
    private static final Logger logger = LogManager.getLogger(SearchUtils.class);

    public SearchUtils() {}

    /**
     * Get connection lookup listener for list of clusters passed
     */
    public static StepListener<BiFunction<String, String, DiscoveryNode>> getConnectionLookupListener(
        RemoteClusterService remoteClusterService,
        ClusterState state,
        Set<String> clusters
    ) {
        final StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = new StepListener<>();

        if (clusters.isEmpty()) {
            lookupListener.onResponse((cluster, nodeId) -> state.getNodes().get(nodeId));
        } else {
            remoteClusterService.collectNodes(clusters, lookupListener);
        }
        return lookupListener;
    }

    /**
     * Delete list of pit contexts. Returns success only if each reader context is either deleted or not found.
     */
    public static void deletePitContexts(
        Map<String, List<SearchContextIdForNode>> nodeToContextsMap,
        ActionListener<Integer> listener,
        ClusterState state,
        SearchTransportService searchTransportService
    ) {
        final Set<String> clusters = nodeToContextsMap.values()
            .stream()
            .flatMap(Collection::stream)
            .filter(ctx -> Strings.isEmpty(ctx.getClusterAlias()) == false)
            .map(SearchContextIdForNode::getClusterAlias)
            .collect(Collectors.toSet());
        StepListener<BiFunction<String, String, DiscoveryNode>> lookupListener = getConnectionLookupListener(
            searchTransportService.getRemoteClusterService(),
            state,
            clusters
        );
        lookupListener.whenComplete(nodeLookup -> {
            final GroupedActionListener<Boolean> groupedListener = new GroupedActionListener<>(
                ActionListener.delegateFailure(
                    listener,
                    (l, result) -> l.onResponse(Math.toIntExact(result.stream().filter(r -> r).count()))
                ),
                nodeToContextsMap.size()
            );

            for (Map.Entry<String, List<SearchContextIdForNode>> entry : nodeToContextsMap.entrySet()) {
                String clusterAlias = entry.getValue().get(0).getClusterAlias();
                final DiscoveryNode node = nodeLookup.apply(clusterAlias, entry.getValue().get(0).getNode());
                if (node == null) {
                    groupedListener.onFailure(new OpenSearchException("node [" + entry.getValue().get(0).getNode() + "] not found"));
                } else {
                    try {
                        final Transport.Connection connection = searchTransportService.getConnection(clusterAlias, node);
                        List<ShardSearchContextId> contextIds = entry.getValue()
                            .stream()
                            .map(r -> r.getSearchContextId())
                            .collect(Collectors.toList());
                        searchTransportService.sendFreePITContexts(
                            connection,
                            contextIds,
                            ActionListener.wrap(r -> groupedListener.onResponse(r.isFreed()), e -> groupedListener.onResponse(false))
                        );
                    } catch (Exception e) {
                        logger.error(() -> new ParameterizedMessage("Delete PITs failed on node [{}]", node.getName()), e);
                        groupedListener.onResponse(false);
                    }
                }
            }
        }, listener::onFailure);
    }
}
