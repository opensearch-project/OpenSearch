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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    final List<String> dimensionNames;
    // The name of the cache type producing these stats. Returned in API response.
    final String storeName;
    public static String STORE_NAME_FIELD = "store_name";

    public ImmutableCacheStatsHolder(Node statsRoot, List<String> dimensionNames, String storeName) {
        this.statsRoot = statsRoot;
        this.dimensionNames = dimensionNames;
        this.storeName = storeName;
    }

    public ImmutableCacheStatsHolder(StreamInput in) throws IOException {
        // Because we write in preorder order, the parent of the next node we read will always be one of the ancestors
        // of the last node we read. This allows us to avoid ambiguity if nodes have the same dimension value, without
        // having to serialize the whole path to each node.
        this.dimensionNames = List.of(in.readStringArray());
        this.statsRoot = new Node("", false, new ImmutableCacheStats(0, 0, 0, 0, 0));
        List<Node> ancestorsOfLastRead = List.of(statsRoot);
        while (ancestorsOfLastRead != null) {
            ancestorsOfLastRead = readAndAttachDimensionNode(in, ancestorsOfLastRead);
        }
        // Finally, update sum-of-children stats for the root node
        CacheStats totalStats = new CacheStats();
        for (Node child : statsRoot.children.values()) {
            totalStats.add(child.getStats());
        }
        statsRoot.incrementStats(totalStats.immutableSnapshot());
        this.storeName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Write each node in preorder order, along with its depth.
        // Then, when rebuilding the tree from the stream, we can always find the correct parent to attach each node to.
        out.writeStringArray(dimensionNames.toArray(new String[0]));
        for (Node child : statsRoot.children.values()) {
            writeDimensionNodeRecursive(out, child, 1);
        }
        out.writeBoolean(false); // Write false to signal there are no more nodes
        out.writeString(storeName);
    }

    private void writeDimensionNodeRecursive(StreamOutput out, Node node, int depth) throws IOException {
        out.writeBoolean(true); // Signals there is a following node to deserialize
        out.writeVInt(depth);
        out.writeString(node.getDimensionValue());
        node.getStats().writeTo(out);

        if (!node.children.isEmpty()) {
            // Not a leaf node
            out.writeBoolean(false); // Write true to indicate this is not a leaf node
            for (Node child : node.children.values()) {
                writeDimensionNodeRecursive(out, child, depth + 1);
            }
        } else {
            out.writeBoolean(true); // Write true to indicate this is a leaf node
        }
    }

    /**
     * Reads a serialized dimension node, attaches it to its appropriate place in the tree, and returns the list of
     * ancestors of the newly attached node.
     */
    private List<Node> readAndAttachDimensionNode(StreamInput in, List<Node> ancestorsOfLastRead) throws IOException {
        boolean hasNextNode = in.readBoolean();
        if (hasNextNode) {
            int depth = in.readVInt();
            String nodeDimensionValue = in.readString();
            ImmutableCacheStats stats = new ImmutableCacheStats(in);
            boolean isLeafNode = in.readBoolean();

            Node result = new Node(nodeDimensionValue, isLeafNode, stats);
            Node parent = ancestorsOfLastRead.get(depth - 1);
            parent.getChildren().put(nodeDimensionValue, result);
            List<Node> ancestors = new ArrayList<>(ancestorsOfLastRead.subList(0, depth));
            ancestors.add(result);
            return ancestors;
        } else {
            // No more nodes
            return null;
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

    public long getTotalEntries() {
        return getTotalStats().getEntries();
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
    Node aggregateByLevels(List<String> filteredLevels) {
        if (filteredLevels.isEmpty()) {
            throw new IllegalArgumentException("Filtered levels passed to aggregateByLevels() cannot be empty");
        }
        Node newRoot = new Node("", false, statsRoot.getStats());
        for (Node child : statsRoot.children.values()) {
            aggregateByLevelsHelper(newRoot, child, filteredLevels, 0);
        }
        return newRoot;
    }

    /**
     * Because we may have to combine nodes that have the same dimension name, I don't think there's a clean way to aggregate
     * fully recursively while also passing in a completed map of children nodes before constructing the parent node.
     * For this reason, in this function we have to build the new tree top down rather than bottom up.
     * We use private methods allowing us to add children to/increment the stats for an existing node.
     * This should be ok because the resulting trees are short-lived objects that are not exposed anywhere outside this class,
     * and the original tree is never changed.
     */
    private void aggregateByLevelsHelper(Node parentInNewTree, Node currentInOriginalTree, List<String> levels, int depth) {
        if (levels.contains(dimensionNames.get(depth))) {
            // If this node is in a level we want to aggregate, create a new dimension node with the same value and stats, and connect it to
            // the last parent node in the new tree. If it already exists, increment it instead.
            String dimensionValue = currentInOriginalTree.getDimensionValue();
            Node nodeInNewTree = parentInNewTree.children.get(dimensionValue);
            if (nodeInNewTree == null) {
                // Create new node with stats matching the node from the original tree
                int indexOfLastLevel = dimensionNames.indexOf(levels.get(levels.size() - 1));
                boolean isLeafNode = depth == indexOfLastLevel; // If this is the last level we aggregate, the new node should be a leaf
                // node
                nodeInNewTree = new Node(dimensionValue, isLeafNode, currentInOriginalTree.getStats());
                parentInNewTree.addChild(dimensionValue, nodeInNewTree);
            } else {
                // Otherwise increment existing stats
                nodeInNewTree.incrementStats(currentInOriginalTree.getStats());
            }
            // Finally set the parent node to be this node for the next callers of this function
            parentInNewTree = nodeInNewTree;
        }

        if (!currentInOriginalTree.children.isEmpty()) {
            // Not a leaf node
            for (Map.Entry<String, Node> childEntry : currentInOriginalTree.children.entrySet()) {
                Node child = childEntry.getValue();
                aggregateByLevelsHelper(parentInNewTree, child, levels, depth + 1);
            }
        }
    }

    /**
     * Filters out levels that aren't in dimensionNames, and orders the resulting list to match the order in dimensionNames.
     * Unrecognized levels are ignored.
     */
    private List<String> filterLevels(List<String> levels) {
        if (levels == null) {
            return new ArrayList<>();
        }
        List<String> filtered = new ArrayList<>();
        for (String dimensionName : dimensionNames) {
            if (levels.contains(dimensionName)) {
                filtered.add(dimensionName);
            }
        }
        return filtered;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        // Always show total stats, regardless of levels
        getTotalStats().toXContent(builder, params);

        List<String> filteredLevels = filterLevels(getLevels(params));
        if (!filteredLevels.isEmpty()) {
            toXContentForLevels(builder, params, filteredLevels);
        }

        // Also add the store name for the cache that produced the stats
        builder.field(STORE_NAME_FIELD, storeName);
        return builder;
    }

    XContentBuilder toXContentForLevels(XContentBuilder builder, Params params, List<String> filteredLevels) throws IOException {
        Node aggregated = aggregateByLevels(filteredLevels);
        // Depth -1 corresponds to the dummy root node
        toXContentForLevelsHelper(-1, aggregated, filteredLevels, builder, params);
        return builder;
    }

    private void toXContentForLevelsHelper(int depth, Node current, List<String> levels, XContentBuilder builder, Params params)
        throws IOException {
        if (depth >= 0) {
            builder.startObject(current.dimensionValue);
        }

        if (depth == levels.size() - 1) {
            // This is a leaf node
            current.getStats().toXContent(builder, params);
        } else {
            builder.startObject(levels.get(depth + 1));
            for (Node nextNode : current.children.values()) {
                toXContentForLevelsHelper(depth + 1, nextNode, levels, builder, params);
            }
            builder.endObject();
        }

        if (depth >= 0) {
            builder.endObject();
        }
    }

    private List<String> getLevels(Params params) {
        String levels = params.param("level");
        if (levels == null) {
            return null;
        }
        return List.of(levels.split(","));
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

    // A similar class to CacheStatsHolder.Node, which uses an ordered TreeMap and holds immutable CacheStatsSnapshot as its stats.
    static class Node {
        private final String dimensionValue;
        final Map<String, Node> children; // Map from dimensionValue to the Node for that dimension value

        // The stats for this node. If a leaf node, corresponds to the stats for this combination of dimensions; if not,
        // contains the sum of its children's stats.
        private ImmutableCacheStats stats;
        private static final Map<String, Node> EMPTY_CHILDREN_MAP = Collections.unmodifiableMap(new HashMap<>());

        Node(String dimensionValue, TreeMap<String, Node> snapshotChildren, ImmutableCacheStats stats) {
            this.dimensionValue = dimensionValue;
            this.stats = stats;
            if (snapshotChildren == null) {
                this.children = EMPTY_CHILDREN_MAP;
            } else {
                this.children = Collections.unmodifiableMap(snapshotChildren);
            }
        }

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
