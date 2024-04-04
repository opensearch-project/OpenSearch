/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * A CacheStats object supporting aggregation over multiple different dimensions.
 * Stores a fixed snapshot of a cache's stats; does not allow changes.
 *
 * @opensearch.experimental
 */
public class MultiDimensionCacheStats implements CacheStats {
    // A snapshot of a StatsHolder containing stats maintained by the cache.
    // Pkg-private for testing.
    final MDCSDimensionNode statsRoot;
    final List<String> dimensionNames;

    public MultiDimensionCacheStats(MDCSDimensionNode statsRoot, List<String> dimensionNames) {
        this.statsRoot = statsRoot;
        this.dimensionNames = dimensionNames;
    }

    public MultiDimensionCacheStats(StreamInput in) throws IOException {
        // Because we write in preorder order, the parent of the next node we read will always be one of the ancestors
        // of the last node we read. This allows us to avoid ambiguity if nodes have the same dimension value, without
        // having to serialize the whole path to each node.
        this.dimensionNames = List.of(in.readStringArray());
        this.statsRoot = new MDCSDimensionNode(null);
        statsRoot.createChildrenMap();
        List<MDCSDimensionNode> ancestorsOfLastRead = List.of(statsRoot);
        while (ancestorsOfLastRead != null) {
            ancestorsOfLastRead = readAndAttachDimensionNode(in, ancestorsOfLastRead);
        }
        // Finally, update sum-of-children stats for the root node
        CacheStatsCounter totalStats = new CacheStatsCounter();
        for (MDCSDimensionNode child : statsRoot.children.values()) {
            totalStats.add(child.getStats());
        }
        statsRoot.setStats(totalStats.snapshot());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Write each node in preorder order, along with its depth.
        // Then, when rebuilding the tree from the stream, we can always find the correct parent to attach each node to.
        out.writeStringArray(dimensionNames.toArray(new String[0]));
        for (MDCSDimensionNode child : statsRoot.children.values()) {
            writeDimensionNodeRecursive(out, child, 1);
        }
        out.writeBoolean(false); // Write false to signal there are no more nodes
    }

    private void writeDimensionNodeRecursive(StreamOutput out, MDCSDimensionNode node, int depth) throws IOException {
        out.writeBoolean(true); // Signals there is a following node to deserialize
        out.writeVInt(depth);
        out.writeString(node.getDimensionValue());
        node.getStats().writeTo(out);

        if (node.hasChildren()) {
            // Not a leaf node
            out.writeBoolean(true); // Write true to indicate we should re-create a map on deserialization
            for (MDCSDimensionNode child : node.children.values()) {
                writeDimensionNodeRecursive(out, child, depth + 1);
            }
        } else {
            out.writeBoolean(false); // Write false to indicate we should not re-create a map on deserialization
        }
    }

    /**
     * Reads a serialized dimension node, attaches it to its appropriate place in the tree, and returns the list of
     * ancestors of the newly attached node.
     */
    private List<MDCSDimensionNode> readAndAttachDimensionNode(StreamInput in, List<MDCSDimensionNode> ancestorsOfLastRead)
        throws IOException {
        boolean hasNextNode = in.readBoolean();
        if (hasNextNode) {
            int depth = in.readVInt();
            String nodeDimensionValue = in.readString();
            CacheStatsCounterSnapshot stats = new CacheStatsCounterSnapshot(in);
            boolean doRecreateMap = in.readBoolean();

            MDCSDimensionNode result = new MDCSDimensionNode(nodeDimensionValue, stats);
            if (doRecreateMap) {
                result.createChildrenMap();
            }
            MDCSDimensionNode parent = ancestorsOfLastRead.get(depth - 1);
            parent.getChildren().put(nodeDimensionValue, result);
            List<MDCSDimensionNode> ancestors = new ArrayList<>(ancestorsOfLastRead.subList(0, depth));
            ancestors.add(result);
            return ancestors;
        } else {
            // No more nodes
            return null;
        }
    }

    @Override
    public CacheStatsCounterSnapshot getTotalStats() {
        return statsRoot.getStats();
    }

    @Override
    public long getTotalHits() {
        return getTotalStats().getHits();
    }

    @Override
    public long getTotalMisses() {
        return getTotalStats().getMisses();
    }

    @Override
    public long getTotalEvictions() {
        return getTotalStats().getEvictions();
    }

    @Override
    public long getTotalSizeInBytes() {
        return getTotalStats().getSizeInBytes();
    }

    @Override
    public long getTotalEntries() {
        return getTotalStats().getEntries();
    }

    /**
     * Returns a new tree containing the stats aggregated by the levels passed in. The root node is a dummy node,
     * whose name and value are null. The new tree only has dimensions matching the levels passed in.
     */
    MDCSDimensionNode aggregateByLevels(List<String> levels) {
        List<String> filteredLevels = filterLevels(levels);
        MDCSDimensionNode newRoot = new MDCSDimensionNode(null, statsRoot.getStats());
        newRoot.createChildrenMap();
        for (MDCSDimensionNode child : statsRoot.children.values()) {
            aggregateByLevelsHelper(newRoot, child, filteredLevels, 0);
        }
        return newRoot;
    }

    void aggregateByLevelsHelper(
        MDCSDimensionNode parentInNewTree,
        MDCSDimensionNode currentInOriginalTree,
        List<String> levels,
        int depth
    ) {
        if (levels.contains(dimensionNames.get(depth))) {
            // If this node is in a level we want to aggregate, create a new dimension node with the same value and stats, and connect it to
            // the last parent node in the new tree. If it already exists, increment it instead.
            String dimensionValue = currentInOriginalTree.getDimensionValue();
            if (parentInNewTree.getChildren() == null) {
                parentInNewTree.createChildrenMap();
            }
            MDCSDimensionNode nodeInNewTree = parentInNewTree.children.get(dimensionValue);
            if (nodeInNewTree == null) {
                // Create new node with stats matching the node from the original tree
                nodeInNewTree = new MDCSDimensionNode(dimensionValue, currentInOriginalTree.getStats());
                parentInNewTree.children.put(dimensionValue, nodeInNewTree);
            } else {
                // Otherwise increment existing stats
                CacheStatsCounterSnapshot newStats = CacheStatsCounterSnapshot.addSnapshots(
                    nodeInNewTree.getStats(),
                    currentInOriginalTree.getStats()
                );
                nodeInNewTree.setStats(newStats);
            }
            // Finally set the parent node to be this node for the next callers of this function
            parentInNewTree = nodeInNewTree;
        }

        if (currentInOriginalTree.hasChildren()) {
            // Not a leaf node
            for (Map.Entry<String, MDCSDimensionNode> childEntry : currentInOriginalTree.children.entrySet()) {
                MDCSDimensionNode child = childEntry.getValue();
                aggregateByLevelsHelper(parentInNewTree, child, levels, depth + 1);
            }
        }
    }

    /**
     * Filters out levels that aren't in dimensionNames. Unrecognized levels are ignored.
     */
    private List<String> filterLevels(List<String> levels) {
        List<String> filtered = new ArrayList<>();
        for (String level : levels) {
            if (dimensionNames.contains(level)) {
                filtered.add(level);
            }
        }
        if (filtered.isEmpty()) {
            throw new IllegalArgumentException("Levels cannot have size 0");
        }
        return filtered;
    }

    // A version of DimensionNode which uses an ordered TreeMap and holds immutable CacheStatsCounterSnapshot as its stats.
    // TODO: Make this extend from DimensionNode?
    static class MDCSDimensionNode {
        private final String dimensionValue;
        TreeMap<String, MDCSDimensionNode> children; // Ordered map from dimensionValue to the DimensionNode for that dimension value

        // The stats for this node. If a leaf node, corresponds to the stats for this combination of dimensions; if not,
        // contains the sum of its children's stats.
        private CacheStatsCounterSnapshot stats;

        MDCSDimensionNode(String dimensionValue) {
            this.dimensionValue = dimensionValue;
            this.children = null; // Lazy load this as needed
            this.stats = null;
        }

        MDCSDimensionNode(String dimensionValue, CacheStatsCounterSnapshot stats) {
            this.dimensionValue = dimensionValue;
            this.children = null;
            this.stats = stats;
        }

        protected void createChildrenMap() {
            children = new TreeMap<>();
        }

        protected Map<String, MDCSDimensionNode> getChildren() {
            return children;
        }

        public CacheStatsCounterSnapshot getStats() {
            return stats;
        }

        public void setStats(CacheStatsCounterSnapshot stats) {
            this.stats = stats;
        }

        public String getDimensionValue() {
            return dimensionValue;
        }

        public boolean hasChildren() {
            return getChildren() != null && !getChildren().isEmpty();
        }
    }

    // pkg-private for testing
    MDCSDimensionNode getStatsRoot() {
        return statsRoot;
    }

    // TODO (in API PR): Produce XContent based on aggregateByLevels()
}
