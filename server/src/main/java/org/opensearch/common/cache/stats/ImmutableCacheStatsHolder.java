/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.Stack;
import java.util.TreeMap;

/**
 * An object storing an immutable snapshot of an entire cache's stats. Accessible outside the cache itself.
 *
 * @opensearch.experimental
 */

@ExperimentalApi
public class ImmutableCacheStatsHolder implements Writeable, ToXContent {
    // Root node of immutable snapshot of stats within a CacheStatsHolder, containing all the stats maintained by the cache.
    // Pkg-private for testing.
    final Node statsRoot;
    // The dimension names for each level in this tree.
    final List<String> dimensionNames;
    // The name of the cache type producing these stats. Returned in API response.
    final String storeName;
    public static String STORE_NAME_FIELD = "store_name";

    // Values used for serializing/deserializing the tree.
    private static final String SERIALIZATION_CHILDREN_OPEN_BRACKET = "<";
    private static final String SERIALIZATION_CHILDREN_CLOSE_BRACKET = ">";
    private static final String SERIALIZATION_BEGIN_NODE = "_";
    private static final String SERIALIZATION_DONE = "end";

    ImmutableCacheStatsHolder(
        DefaultCacheStatsHolder.Node originalStatsRoot,
        String[] levels,
        List<String> originalDimensionNames,
        String storeName
    ) {
        // Aggregate from the original CacheStatsHolder according to the levels passed in.
        // The dimension names for this immutable snapshot should reflect the levels we aggregate in the snapshot
        this.dimensionNames = filterLevels(levels, originalDimensionNames);
        this.storeName = storeName;
        this.statsRoot = aggregateByLevels(originalStatsRoot, originalDimensionNames);
        makeNodeUnmodifiable(statsRoot);
    }

    public ImmutableCacheStatsHolder(StreamInput in) throws IOException {
        this.dimensionNames = List.of(in.readStringArray());
        this.storeName = in.readString();
        this.statsRoot = deserializeTree(in);
        makeNodeUnmodifiable(statsRoot);
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringArray(dimensionNames.toArray(new String[0]));
        out.writeString(storeName);
        writeNode(statsRoot, out);
        out.writeString(SERIALIZATION_DONE);
    }

    private void writeNode(Node node, StreamOutput out) throws IOException {
        out.writeString(SERIALIZATION_BEGIN_NODE);
        out.writeString(node.dimensionValue);
        out.writeBoolean(node.children.isEmpty()); // Write whether this is a leaf node
        node.stats.writeTo(out);

        out.writeString(SERIALIZATION_CHILDREN_OPEN_BRACKET);
        for (Map.Entry<String, Node> entry : node.children.entrySet()) {
            out.writeString(entry.getKey());
            writeNode(entry.getValue(), out);
        }
        out.writeString(SERIALIZATION_CHILDREN_CLOSE_BRACKET);
    }

    private Node deserializeTree(StreamInput in) throws IOException {
        final Stack<Node> stack = new Stack<>();
        in.readString(); // Read and discard SERIALIZATION_BEGIN_NODE for the root node
        Node statsRoot = readSingleNode(in);
        Node current = statsRoot;
        stack.push(statsRoot);
        String nextSymbol = in.readString();
        while (!nextSymbol.equals(SERIALIZATION_DONE)) {
            switch (nextSymbol) {
                case SERIALIZATION_CHILDREN_OPEN_BRACKET:
                    stack.push(current);
                    break;
                case SERIALIZATION_CHILDREN_CLOSE_BRACKET:
                    stack.pop();
                    break;
                case SERIALIZATION_BEGIN_NODE:
                    current = readSingleNode(in);
                    stack.peek().children.put(current.dimensionValue, current);
            }
            nextSymbol = in.readString();
        }
        return statsRoot;
    }

    private Node readSingleNode(StreamInput in) throws IOException {
        String dimensionValue = in.readString();
        boolean isLeafNode = in.readBoolean();
        ImmutableCacheStats stats = new ImmutableCacheStats(in);
        return new Node(dimensionValue, isLeafNode, stats);
    }

    private void makeNodeUnmodifiable(Node node) {
        if (!node.children.isEmpty()) {
            node.children = Collections.unmodifiableSortedMap(node.children);
        }
        for (Node child : node.children.values()) {
            makeNodeUnmodifiable(child);
        }
    }

    public ImmutableCacheStats getTotalStats() {
        return statsRoot.getStats();
    }

    public long getTotalHits() {
        return getTotalStats().getHits();
    }

    public long getTotalMisses() {
        return getTotalStats().getMisses();
    }

    public long getTotalEvictions() {
        return getTotalStats().getEvictions();
    }

    public long getTotalSizeInBytes() {
        return getTotalStats().getSizeInBytes();
    }

    public long getTotalItems() {
        return getTotalStats().getItems();
    }

    public ImmutableCacheStats getStatsForDimensionValues(List<String> dimensionValues) {
        Node current = statsRoot;
        for (String dimensionValue : dimensionValues) {
            current = current.children.get(dimensionValue);
            if (current == null) {
                return null;
            }
        }
        return current.stats;
    }

    /**
     * Returns a new tree containing the stats aggregated by the levels passed in.
     * The new tree only has dimensions matching the levels passed in.
     * The levels passed in must be in the proper order, as they would be in the output of filterLevels().
     */
    Node aggregateByLevels(DefaultCacheStatsHolder.Node originalStatsRoot, List<String> originalDimensionNames) {
        Node newRoot = new Node("", false, originalStatsRoot.getImmutableStats());
        for (DefaultCacheStatsHolder.Node child : originalStatsRoot.children.values()) {
            aggregateByLevelsHelper(newRoot, child, originalDimensionNames, 0);
        }
        return newRoot;
    }

    /**
     * Because we may have to combine nodes that have the same dimension name, I don't think there's a clean way to aggregate
     * fully recursively while also passing in a completed map of children nodes before constructing the parent node.
     * For this reason, in this function we have to build the new tree top down rather than bottom up.
     * We use private methods allowing us to add children to/increment the stats for an existing node.
     * This should be ok because the resulting tree is unmodifiable after creation in the constructor.
     *
     * @param allDimensions the list of all dimensions present in the original CacheStatsHolder which produced
     *                      the CacheStatsHolder.Node object we are traversing.
     */
    private void aggregateByLevelsHelper(
        Node parentInNewTree,
        DefaultCacheStatsHolder.Node currentInOriginalTree,
        List<String> allDimensions,
        int depth
    ) {
        if (dimensionNames.contains(allDimensions.get(depth))) {
            // If this node is in a level we want to aggregate, create a new dimension node with the same value and stats, and connect it to
            // the last parent node in the new tree. If it already exists, increment it instead.
            String dimensionValue = currentInOriginalTree.getDimensionValue();
            Node nodeInNewTree = parentInNewTree.children.get(dimensionValue);
            if (nodeInNewTree == null) {
                // Create new node with stats matching the node from the original tree
                int indexOfLastLevel = allDimensions.indexOf(dimensionNames.get(dimensionNames.size() - 1));
                boolean isLeafNode = depth == indexOfLastLevel; // If this is the last level we aggregate, the new node should be a leaf
                // node
                nodeInNewTree = new Node(dimensionValue, isLeafNode, currentInOriginalTree.getImmutableStats());
                parentInNewTree.addChild(dimensionValue, nodeInNewTree);
            } else {
                // Otherwise increment existing stats
                nodeInNewTree.incrementStats(currentInOriginalTree.getImmutableStats());
            }
            // Finally set the parent node to be this node for the next callers of this function
            parentInNewTree = nodeInNewTree;
        }

        for (Map.Entry<String, DefaultCacheStatsHolder.Node> childEntry : currentInOriginalTree.children.entrySet()) {
            DefaultCacheStatsHolder.Node child = childEntry.getValue();
            aggregateByLevelsHelper(parentInNewTree, child, allDimensions, depth + 1);
        }
    }

    /**
     * Filters out levels that aren't in dimensionNames, and orders the resulting list to match the order in dimensionNames.
     * Unrecognized levels are ignored.
     */
    private List<String> filterLevels(String[] levels, List<String> originalDimensionNames) {
        if (levels == null) {
            return originalDimensionNames;
        }
        List<String> levelsList = Arrays.asList(levels);
        List<String> filtered = new ArrayList<>();
        for (String dimensionName : originalDimensionNames) {
            if (levelsList.contains(dimensionName)) {
                filtered.add(dimensionName);
            }
        }
        return filtered;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // Always show total stats, regardless of levels
        getTotalStats().toXContent(builder, params);

        List<String> filteredLevels = filterLevels(getLevels(params), dimensionNames);
        assert filteredLevels.equals(dimensionNames);
        if (!filteredLevels.isEmpty()) {
            // Depth -1 corresponds to the dummy root node
            toXContentForLevels(-1, statsRoot, builder, params);
        }

        // Also add the store name for the cache that produced the stats
        builder.field(STORE_NAME_FIELD, storeName);
        return builder;
    }

    private void toXContentForLevels(int depth, Node current, XContentBuilder builder, Params params) throws IOException {
        if (depth >= 0) {
            builder.startObject(current.dimensionValue);
        }

        if (depth == dimensionNames.size() - 1) {
            // This is a leaf node
            current.getStats().toXContent(builder, params);
        } else {
            builder.startObject(dimensionNames.get(depth + 1));
            for (Node nextNode : current.children.values()) {
                toXContentForLevels(depth + 1, nextNode, builder, params);
            }
            builder.endObject();
        }

        if (depth >= 0) {
            builder.endObject();
        }
    }

    private String[] getLevels(Params params) {
        String levels = params.param("level");
        if (levels == null) {
            return null;
        }
        return levels.split(",");
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || o.getClass() != ImmutableCacheStatsHolder.class) {
            return false;
        }
        ImmutableCacheStatsHolder other = (ImmutableCacheStatsHolder) o;
        if (!dimensionNames.equals(other.dimensionNames) || !storeName.equals(other.storeName)) {
            return false;
        }
        return equalsHelper(statsRoot, other.getStatsRoot());
    }

    private boolean equalsHelper(Node thisNode, Node otherNode) {
        if (otherNode == null) {
            return false;
        }
        if (!thisNode.getStats().equals(otherNode.getStats())) {
            return false;
        }
        boolean allChildrenMatch = true;
        for (String childValue : thisNode.getChildren().keySet()) {
            allChildrenMatch = equalsHelper(thisNode.children.get(childValue), otherNode.children.get(childValue));
            if (!allChildrenMatch) {
                return false;
            }
        }
        return allChildrenMatch;
    }

    @Override
    public int hashCode() {
        // Should be sufficient to hash based on the total stats value (found in the root node)
        return Objects.hash(statsRoot.stats, dimensionNames);
    }

    // A similar class to CacheStatsHolder.Node, which uses a SortedMap and holds immutable CacheStatsSnapshot as its stats.
    static class Node {
        private final String dimensionValue;
        // Map from dimensionValue to the Node for that dimension value. Not final so we can set it to be unmodifiable before we are done in
        // the constructor.
        SortedMap<String, Node> children;

        // The stats for this node. If a leaf node, corresponds to the stats for this combination of dimensions; if not,
        // contains the sum of its children's stats.
        private ImmutableCacheStats stats;
        private static final SortedMap<String, Node> EMPTY_CHILDREN_MAP = Collections.unmodifiableSortedMap(new TreeMap<>());

        private Node(String dimensionValue, boolean isLeafNode, ImmutableCacheStats stats) {
            this.dimensionValue = dimensionValue;
            this.stats = stats;
            if (isLeafNode) {
                this.children = EMPTY_CHILDREN_MAP;
            } else {
                this.children = new TreeMap<>();
            }
        }

        Map<String, Node> getChildren() {
            return children;
        }

        public ImmutableCacheStats getStats() {
            return stats;
        }

        public String getDimensionValue() {
            return dimensionValue;
        }

        private void addChild(String dimensionValue, Node child) {
            this.children.putIfAbsent(dimensionValue, child);
        }

        private void incrementStats(ImmutableCacheStats toIncrement) {
            stats = ImmutableCacheStats.addSnapshots(stats, toIncrement);
        }
    }

    // pkg-private for testing
    Node getStatsRoot() {
        return statsRoot;
    }
}
