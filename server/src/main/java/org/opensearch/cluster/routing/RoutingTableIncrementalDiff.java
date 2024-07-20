/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.Diff;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a difference between {@link IndexRoutingTable} objects that can be serialized and deserialized.
 */
public class RoutingTableIncrementalDiff implements Diff<IndexRoutingTable> {
    private final Map<String, Diff<IndexRoutingTable>> diffs;

    /**
     * Constructs a new RoutingTableIncrementalDiff with the given differences.
     *
     * @param diffs a map containing the differences of {@link IndexRoutingTable}.
     */
    public RoutingTableIncrementalDiff(Map<String, Diff<IndexRoutingTable>> diffs) {
        this.diffs = diffs;
    }

    /**
     * Gets the map of differences of {@link IndexRoutingTable}.
     *
     * @return a map containing the differences.
     */
    public Map<String, Diff<IndexRoutingTable>> getDiffs() {
        return diffs;
    }

    /**
     * Reads a {@link RoutingTableIncrementalDiff} from the given {@link StreamInput}.
     *
     * @param in the input stream to read from.
     * @return the deserialized RoutingTableIncrementalDiff.
     * @throws IOException if an I/O exception occurs while reading from the stream.
     */
    public static RoutingTableIncrementalDiff readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        Map<String, Diff<IndexRoutingTable>> diffs = new HashMap<>();

        for (int i = 0; i < size; i++) {
            String key = in.readString();
            List<IndexShardRoutingTable> shardRoutingTables = new ArrayList<>();

            // Read each IndexShardRoutingTable from the stream
            int numShards = in.readVInt();
            for (int j = 0; j < numShards; j++) {
                IndexShardRoutingTable shardRoutingTable = IndexShardRoutingTable.Builder.readFrom(in);
                shardRoutingTables.add(shardRoutingTable);
            }

            // Create a diff object for the index
            Diff<IndexRoutingTable> diff = new IndexRoutingTableIncrementalDiff(shardRoutingTables);

            // Put the diff into the map with the key
            diffs.put(key, diff);
        }
        return new RoutingTableIncrementalDiff(diffs);
    }

    /**
     * Applies the differences to the provided {@link IndexRoutingTable}.
     *
     * @param part the original IndexRoutingTable to which the differences will be applied.
     * @return the updated IndexRoutingTable with the applied differences.
     */
    @Override
    public IndexRoutingTable apply(IndexRoutingTable part) {
        // Apply diffs to the provided IndexRoutingTable
        for (Map.Entry<String, Diff<IndexRoutingTable>> entry : diffs.entrySet()) {
            part = entry.getValue().apply(part);
        }
        return part;
    }

    /**
     * Writes the differences to the given {@link StreamOutput}.
     *
     * @param out the output stream to write to.
     * @throws IOException if an I/O exception occurs while writing to the stream.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(diffs.size());
        for (Map.Entry<String, Diff<IndexRoutingTable>> entry : diffs.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeTo(out);
        }
    }

    /**
     * Represents a difference between {@link IndexShardRoutingTable} objects that can be serialized and deserialized.
     */
    public static class IndexRoutingTableIncrementalDiff implements Diff<IndexRoutingTable> {

        private final List<IndexShardRoutingTable> indexShardRoutingTables;

        /**
         * Constructs a new IndexShardRoutingTableDiff with the given shard routing tables.
         *
         * @param indexShardRoutingTables a list of IndexShardRoutingTable representing the differences.
         */
        public IndexRoutingTableIncrementalDiff(List<IndexShardRoutingTable> indexShardRoutingTables) {
            this.indexShardRoutingTables = indexShardRoutingTables;
        }

        /**
         * Applies the differences to the provided {@link IndexRoutingTable}.
         *
         * @param part the original IndexRoutingTable to which the differences will be applied.
         * @return the updated IndexRoutingTable with the applied differences.
         */
        @Override
        public IndexRoutingTable apply(IndexRoutingTable part) {
            IndexRoutingTable.Builder builder = new IndexRoutingTable.Builder(part.getIndex());
            for (IndexShardRoutingTable shardRoutingTable : part) {
                builder.addIndexShard(shardRoutingTable); // Add existing shards to builder
            }

            // Apply the diff: update or add the new shard routing tables
            for (IndexShardRoutingTable diffShard : indexShardRoutingTables) {
                builder.addIndexShard(diffShard);
            }
            return builder.build();
        }

        /**
         * Writes the differences to the given {@link StreamOutput}.
         *
         * @param out the output stream to write to.
         * @throws IOException if an I/O exception occurs while writing to the stream.
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(indexShardRoutingTables.size());
            for (IndexShardRoutingTable shardRoutingTable : indexShardRoutingTables) {
                IndexShardRoutingTable.Builder.writeTo(shardRoutingTable, out);
            }
        }

        /**
         * Reads a {@link IndexRoutingTableIncrementalDiff} from the given {@link StreamInput}.
         *
         * @param in the input stream to read from.
         * @return the deserialized IndexShardRoutingTableDiff.
         * @throws IOException if an I/O exception occurs while reading from the stream.
         */
        public static IndexRoutingTableIncrementalDiff readFrom(StreamInput in) throws IOException {
            int size = in.readVInt();
            List<IndexShardRoutingTable> indexShardRoutingTables = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                IndexShardRoutingTable shardRoutingTable = IndexShardRoutingTable.Builder.readFrom(in);
                indexShardRoutingTables.add(shardRoutingTable);
            }
            return new IndexRoutingTableIncrementalDiff(indexShardRoutingTables);
        }
    }
}
