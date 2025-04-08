/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.cluster.routing.allocation.decider.Decision;
import org.opensearch.common.Booleans;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;

import static org.opensearch.cluster.metadata.MetadataIndexStateService.isIndexVerifiedBeforeClosed;

/**
 * This class acts as a functional wrapper around the {@code index.auto_expand_read_replicas} setting.
 * This setting's value expands into a minimum and maximum value, requiring special handling based on the
 * number of search nodes in the cluster. This class handles parsing and simplifies access to these values.
 *
 * @opensearch.internal
 */
public final class AutoExpandSearchReplicas {
    // the value we recognize in the "max" position to mean all the search nodes
    private static final String ALL_NODES_VALUE = "all";

    private static final AutoExpandSearchReplicas FALSE_INSTANCE = new AutoExpandSearchReplicas(0, 0, false);

    public static final Setting<AutoExpandSearchReplicas> SETTING = new Setting<>(
        IndexMetadata.INDEX_AUTO_EXPAND_READ_REPLICAS,
        "false",
        AutoExpandSearchReplicas::parse,
        Property.Dynamic,
        Property.IndexScope
    );

    private static AutoExpandSearchReplicas parse(String value) {
        final int min;
        final int max;
        if (Booleans.isFalse(value)) {
            return FALSE_INSTANCE;
        }
        final int dash = value.indexOf('-');
        if (-1 == dash) {
            throw new IllegalArgumentException(
                "failed to parse [" + IndexMetadata.INDEX_AUTO_EXPAND_READ_REPLICAS + "] from value: [" + value + "] at index " + dash
            );
        }
        final String sMin = value.substring(0, dash);
        try {
            min = Integer.parseInt(sMin);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                "failed to parse [" + IndexMetadata.INDEX_AUTO_EXPAND_READ_REPLICAS + "] from value: [" + value + "] at index " + dash,
                e
            );
        }
        String sMax = value.substring(dash + 1);
        if (sMax.equals(ALL_NODES_VALUE)) {
            max = Integer.MAX_VALUE;
        } else {
            try {
                max = Integer.parseInt(sMax);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                    "failed to parse ["
                        + IndexMetadata.INDEX_AUTO_EXPAND_READ_REPLICAS
                        + "] from value: ["
                        + value
                        + "] at index "
                        + dash,
                    e
                );
            }
        }
        return new AutoExpandSearchReplicas(min, max, true);
    }

    private final int minSearchReplicas;
    private final int maxSearchReplicas;
    private final boolean enabled;

    private AutoExpandSearchReplicas(int minReplicas, int maxReplicas, boolean enabled) {
        if (minReplicas > maxReplicas) {
            throw new IllegalArgumentException(
                "["
                    + IndexMetadata.INDEX_AUTO_EXPAND_READ_REPLICAS
                    + "] minSearchReplicas must be =< maxSearchReplicas but wasn't "
                    + minReplicas
                    + " > "
                    + maxReplicas
            );
        }
        this.minSearchReplicas = minReplicas;
        this.maxSearchReplicas = maxReplicas;
        this.enabled = enabled;
    }

    int getMinSearchReplicas() {
        return minSearchReplicas;
    }

    public int getMaxSearchReplicas() {
        return maxSearchReplicas;
    }

    public boolean isEnabled() {
        return enabled;
    }

    private OptionalInt getDesiredNumberOfSearchReplicas(IndexMetadata indexMetadata, RoutingAllocation allocation) {
        int numMatchingSearchNodes = (int) allocation.nodes()
            .getDataNodes()
            .values()
            .stream()
            .filter(DiscoveryNode::isSearchNode)
            .map(node -> allocation.deciders().shouldAutoExpandToNode(indexMetadata, node, allocation))
            .filter(decision -> decision.type() != Decision.Type.NO)
            .count();

        return calculateNumberOfSearchReplicas(numMatchingSearchNodes);
    }

    // package private for testing
    OptionalInt calculateNumberOfSearchReplicas(int numMatchingSearchNodes) {
        // Calculate the maximum possible number of search replicas
        int maxPossibleReplicas = Math.min(numMatchingSearchNodes, maxSearchReplicas);

        // Determine the number of search replicas
        int numberOfSearchReplicas = Math.max(minSearchReplicas, maxPossibleReplicas);

        // Additional check to ensure we don't exceed max possible search replicas
        if (numberOfSearchReplicas <= maxPossibleReplicas) {
            return OptionalInt.of(numberOfSearchReplicas);
        }

        return OptionalInt.empty();
    }

    @Override
    public String toString() {
        return enabled ? minSearchReplicas + "-" + maxSearchReplicas : "false";
    }

    /**
     * Checks if there are search replicas with the auto-expand feature that need to be adapted.
     * Returns a map of updates, which maps the indices to be updated to the desired number of search replicas.
     * The map has the desired number of search replicas as key and the indices to update as value, as this allows the result
     * of this method to be directly applied to RoutingTable.Builder#updateNumberOfSearchReplicas.
     */
    public static Map<Integer, List<String>> getAutoExpandSearchReplicaChanges(Metadata metadata, RoutingAllocation allocation) {
        Map<Integer, List<String>> updatedSearchReplicas = new HashMap<>();

        for (final IndexMetadata indexMetadata : metadata) {
            if (indexMetadata.getState() == IndexMetadata.State.OPEN || isIndexVerifiedBeforeClosed(indexMetadata)) {
                AutoExpandSearchReplicas autoExpandSearchReplicas = SETTING.get(indexMetadata.getSettings());
                if (autoExpandSearchReplicas.isEnabled()) {
                    autoExpandSearchReplicas.getDesiredNumberOfSearchReplicas(indexMetadata, allocation)
                        .ifPresent(numberOfSearchReplicas -> {
                            if (numberOfSearchReplicas != indexMetadata.getNumberOfSearchOnlyReplicas()) {
                                updatedSearchReplicas.computeIfAbsent(numberOfSearchReplicas, ArrayList::new)
                                    .add(indexMetadata.getIndex().getName());
                            }
                        });
                }
            }
        }
        return updatedSearchReplicas;
    }
}
