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

/**
 * A CacheStats object supporting aggregation over multiple different dimensions.
 * Stores a fixed snapshot of a cache's stats; does not allow changes.
 *
 * @opensearch.experimental
 */
public class MultiDimensionCacheStats implements CacheStats {
    // A snapshot of a StatsHolder containing stats maintained by the cache.
    // Pkg-private for testing.
    final DimensionNode<CounterSnapshot> statsRoot;
    final List<String> dimensionNames;

    public MultiDimensionCacheStats(DimensionNode<CounterSnapshot> statsRoot, List<String> dimensionNames) {
        this.statsRoot = statsRoot;
        this.dimensionNames = dimensionNames;
    }

    public MultiDimensionCacheStats(StreamInput in) throws IOException {
        // Because we write in preorder order, the parent of the next node we read will always be one of the ancestors of the last node we
        // read.
        // This allows us to avoid ambiguity if nodes have the same dimension value, without having to serialize the whole path to each
        // node.
        this.dimensionNames = List.of(in.readStringArray());
        this.statsRoot = new DimensionNode<>(null);
        List<DimensionNode<CounterSnapshot>> ancestorsOfLastRead = List.of(statsRoot);
        while (ancestorsOfLastRead != null) {
            ancestorsOfLastRead = readAndAttachDimensionNode(in, ancestorsOfLastRead);
        }
        // Finally, update sum-of-children stats for the root node
        CacheStatsCounter totalStats = new CacheStatsCounter();
        for (DimensionNode<CounterSnapshot> child : statsRoot.children.values()) {
            totalStats.add(child.getStats());
        }
        statsRoot.setStats(totalStats.snapshot());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Write each node in preorder order, along with its depth.
        // Then, when rebuilding the tree from the stream, we can always find the correct parent to attach each node to.

        out.writeStringArray(dimensionNames.toArray(new String[0]));
        for (DimensionNode<CounterSnapshot> child : statsRoot.children.values()) {
            writeDimensionNodeRecursive(out, child, 1);
        }
        out.writeBoolean(false); // Write false to signal there are no more nodes
    }

    private void writeDimensionNodeRecursive(StreamOutput out, DimensionNode<CounterSnapshot> node, int depth)
        throws IOException {
        out.writeBoolean(true);
        out.writeVInt(depth);
        out.writeString(node.getDimensionValue());
        node.getStats().writeTo(out);

        if (!node.children.isEmpty()) {
            // Not a leaf node
            for (DimensionNode<CounterSnapshot> child : node.children.values()) {
                writeDimensionNodeRecursive(out, child, depth + 1);
            }
        }
    }

    /**
     * Reads a serialized dimension node, attaches it to its appropriate place in the tree, and returns the list of ancestors of the newly attached node.
     */
    private List<DimensionNode<CounterSnapshot>> readAndAttachDimensionNode(
        StreamInput in,
        List<DimensionNode<CounterSnapshot>> ancestorsOfLastRead
    ) throws IOException {
        boolean hasNextNode = in.readBoolean();
        if (hasNextNode) {
            int depth = in.readVInt();
            String nodeDimensionValue = in.readString();
            CounterSnapshot stats = new CounterSnapshot(in);

            DimensionNode<CounterSnapshot> result = new DimensionNode<>(nodeDimensionValue);
            result.setStats(stats);
            DimensionNode<CounterSnapshot> parent = ancestorsOfLastRead.get(depth - 1);
            parent.children.put(nodeDimensionValue, result);
            List<DimensionNode<CounterSnapshot>> ancestors = new ArrayList<>(ancestorsOfLastRead.subList(0, depth));
            ancestors.add(result);
            return ancestors;
        } else {
            // No more nodes
            return null;
        }
    }

    @Override
    public CounterSnapshot getTotalStats() {
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
     * whose name and value are null.
     */
    DimensionNode<CounterSnapshot> aggregateByLevels(List<String> levels) {
        checkLevels(levels);
        DimensionNode<CounterSnapshot> newRoot = new DimensionNode<>(null);
        for (DimensionNode<CounterSnapshot> child : statsRoot.children.values()) {
            aggregateByLevelsHelper(newRoot, child, levels, 0);
        }
        return newRoot;
    }

    void aggregateByLevelsHelper(
        DimensionNode<CounterSnapshot> parentInNewTree,
        DimensionNode<CounterSnapshot> currentInOriginalTree,
        List<String> levels,
        int depth
    ) {
        if (levels.contains(dimensionNames.get(depth))) {
            // If this node is in a level we want to aggregate, create a new dimension node with the same value and stats, and connect it to
            // the last parent node in the new tree.
            // If it already exists, increment it instead.
            String dimensionValue = currentInOriginalTree.getDimensionValue();
            DimensionNode<CounterSnapshot> nodeInNewTree = parentInNewTree.children.get(dimensionValue);
            if (nodeInNewTree == null) {
                nodeInNewTree = new DimensionNode<>(dimensionValue);
                nodeInNewTree.setStats(currentInOriginalTree.getStats());
                parentInNewTree.children.put(dimensionValue, nodeInNewTree);
            } else {
                CounterSnapshot newStats = CounterSnapshot.addSnapshots(nodeInNewTree.getStats(), currentInOriginalTree.getStats());
                nodeInNewTree.setStats(newStats);
            }
            // Finally set the parent node to be this node for the next callers of this function
            parentInNewTree = nodeInNewTree;
        }

        if (!currentInOriginalTree.children.isEmpty()) {
            // Not a leaf node
            for (Map.Entry<String, DimensionNode<CounterSnapshot>> childEntry : currentInOriginalTree.children.entrySet()) {
                DimensionNode<CounterSnapshot> child = childEntry.getValue();
                aggregateByLevelsHelper(parentInNewTree, child, levels, depth + 1);
            }
        }
    }

    private void checkLevels(List<String> levels) {
        if (levels.isEmpty()) {
            throw new IllegalArgumentException("Levels cannot have size 0");
        }
        for (String level : levels) {
            if (!dimensionNames.contains(level)) {
                throw new IllegalArgumentException("Unrecognized level: " + level);
            }
        }
    }

    // pkg-private for testing
    DimensionNode<CounterSnapshot> getStatsRoot() {
        return statsRoot;
    }

    // TODO (in API PR): Produce XContent based on aggregateByLevels()
}
