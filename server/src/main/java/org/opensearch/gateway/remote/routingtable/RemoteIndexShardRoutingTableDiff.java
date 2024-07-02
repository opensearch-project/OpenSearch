/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.routingtable;

import org.opensearch.cluster.Diff;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the differences between {@link IndexRoutingTable}s for uploading and downloading index routing table diff data to/from a remote store.
 *
 * @opensearch.internal
 */
public class RemoteIndexShardRoutingTableDiff implements Diff<IndexRoutingTable> {

    private final List<IndexShardRoutingTable> indexShardRoutingTables;

    /**
     * Constructs a new RemoteIndexShardRoutingTableDiff with the given shard routing tables.
     *
     * @param indexShardRoutingTables the list of {@link IndexShardRoutingTable}s representing the differences.
     */
    public RemoteIndexShardRoutingTableDiff(List<IndexShardRoutingTable> indexShardRoutingTables) {
        this.indexShardRoutingTables = indexShardRoutingTables;
    }

    /**
     * Applies the differences to the provided {@link IndexRoutingTable}.
     *
     * @param part the original {@link IndexRoutingTable}.
     * @return the updated {@link IndexRoutingTable} with the applied differences.
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
     * Writes the shard routing tables to the provided {@link StreamOutput}.
     *
     * @param out the output stream to write to.
     * @throws IOException if an I/O exception occurs while writing.
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(indexShardRoutingTables.size());
        for (IndexShardRoutingTable shardRoutingTable : indexShardRoutingTables) {
            IndexShardRoutingTable.Builder.writeTo(shardRoutingTable, out);
        }
    }

    /**
     * Reads the shard routing tables from the provided {@link StreamInput}.
     *
     * @param in the input stream to read from.
     * @return a new {@link RemoteIndexShardRoutingTableDiff} with the read shard routing tables.
     * @throws IOException if an I/O exception occurs while reading.
     */
    public static RemoteIndexShardRoutingTableDiff readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        List<IndexShardRoutingTable> indexShardRoutingTables = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            IndexShardRoutingTable shardRoutingTable = IndexShardRoutingTable.Builder.readFrom(in);
            indexShardRoutingTables.add(shardRoutingTable);
        }
        return new RemoteIndexShardRoutingTableDiff(indexShardRoutingTables);
    }
}
