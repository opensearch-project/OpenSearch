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
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Strings;
import org.opensearch.transport.RemoteClusterService;
import org.opensearch.transport.Transport;

import java.util.Collection;
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
     * Delete list of pits, return success if all reader contexts are deleted ( or not found ).
     */
    public static void deletePits(
        Collection<SearchContextIdForNode> contexts,
        ActionListener<Integer> listener,
        ClusterState state,
        SearchTransportService searchTransportService
    ) {
        final Set<String> clusters = contexts.stream()
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
                contexts.size()
            );

            for (SearchContextIdForNode contextId : contexts) {
                final DiscoveryNode node = nodeLookup.apply(contextId.getClusterAlias(), contextId.getNode());
                if (node == null) {
                    groupedListener.onFailure(new OpenSearchException("node not found"));
                } else {
                    try {
                        final Transport.Connection connection = searchTransportService.getConnection(contextId.getClusterAlias(), node);
                        searchTransportService.sendFreePITContext(
                            connection,
                            contextId.getSearchContextId(),
                            ActionListener.wrap(r -> groupedListener.onResponse(r.isFreed()), e -> groupedListener.onResponse(false))
                        );
                    } catch (Exception e) {
                        logger.debug("Delete PIT failed ", e);
                        groupedListener.onResponse(false);
                    }
                }
            }
        }, listener::onFailure);
    }
}
