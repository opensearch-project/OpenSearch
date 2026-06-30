/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.Diff;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.cluster.DiffableUtils.MapDiff;

/**
 * Represents a difference between {@link RoutingTable} objects that can be serialized and deserialized.
 */
public class RoutingTableIncrementalDiff implements Diff<RoutingTable>, StringKeyDiffProvider<IndexRoutingTable> {

    private final Diff<Map<String, IndexRoutingTable>> indicesRouting;

    private final long version;

    private static final DiffableUtils.DiffableValueSerializer<String, IndexRoutingTable> CUSTOM_ROUTING_TABLE_DIFFABLE_VALUE_SERIALIZER =
        new DiffableUtils.DiffableValueSerializer<>() {

            @Override
            public IndexRoutingTable read(StreamInput in, String key) throws IOException {
                return IndexRoutingTable.readFrom(in);
            }

            @Override
            public Diff<IndexRoutingTable> readDiff(StreamInput in, String key) throws IOException {
                return new RoutingTableIncrementalDiff.IndexRoutingTableIncrementalDiff(in);
            }

            @Override
            public Diff<IndexRoutingTable> diff(IndexRoutingTable currentState, IndexRoutingTable previousState) {
                return new RoutingTableIncrementalDiff.IndexRoutingTableIncrementalDiff(
                    currentState.getIndex(),
                    previousState,
                    currentState
                );
            }
        };

    public RoutingTableIncrementalDiff(RoutingTable before, RoutingTable after) {
        version = after.version();
        indicesRouting = DiffableUtils.diff(
            before.getIndicesRouting(),
            after.getIndicesRouting(),
            DiffableUtils.getStringKeySerializer(),
            CUSTOM_ROUTING_TABLE_DIFFABLE_VALUE_SERIALIZER
        );
    }

    public RoutingTableIncrementalDiff(StreamInput in) throws IOException {
        version = in.readLong();
        indicesRouting = DiffableUtils.readJdkMapDiff(
            in,
            DiffableUtils.getStringKeySerializer(),
            CUSTOM_ROUTING_TABLE_DIFFABLE_VALUE_SERIALIZER
        );
    }

    public static RoutingTableIncrementalDiff readFrom(StreamInput in) throws IOException {
        return new RoutingTableIncrementalDiff(in);
    }

    @Override
    public RoutingTable apply(RoutingTable part) {
        return new RoutingTable(version, indicesRouting.apply(part.getIndicesRouting()));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(version);
        indicesRouting.writeTo(out);
    }

    @Override
    public MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>> provideDiff() {
        return (MapDiff<String, IndexRoutingTable, Map<String, IndexRoutingTable>>) indicesRouting;
    }

    /**
     * Represents a difference between {@link IndexShardRoutingTable} objects that can be serialized and deserialized.
     */
    public static class IndexRoutingTableIncrementalDiff implements Diff<IndexRoutingTable> {

        private final Diff<Map<Integer, IndexShardRoutingTable>> indexShardRoutingTables;

        private final Index index;

        public IndexRoutingTableIncrementalDiff(Index index, IndexRoutingTable before, IndexRoutingTable after) {
            this.index = index;
            this.indexShardRoutingTables = DiffableUtils.diff(before.getShards(), after.getShards(), DiffableUtils.getIntKeySerializer());
        }

        private static final DiffableUtils.DiffableValueReader<Integer, IndexShardRoutingTable> DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(IndexShardRoutingTable::readFrom, IndexShardRoutingTable::readDiffFrom);

        public IndexRoutingTableIncrementalDiff(StreamInput in) throws IOException {
            this.index = new Index(in);
            this.indexShardRoutingTables = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getIntKeySerializer(), DIFF_VALUE_READER);
        }

        @Override
        public IndexRoutingTable apply(IndexRoutingTable part) {
            return new IndexRoutingTable(index, indexShardRoutingTables.apply(part.getShards()));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            index.writeTo(out);
            indexShardRoutingTables.writeTo(out);
        }

        public static IndexRoutingTableIncrementalDiff readFrom(StreamInput in) throws IOException {
            return new IndexRoutingTableIncrementalDiff(in);
        }
    }
}
