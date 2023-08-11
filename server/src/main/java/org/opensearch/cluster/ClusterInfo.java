/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster;

import org.opensearch.Version;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.remote.filecache.FileCacheStats;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * ClusterInfo is an object representing a map of nodes to {@link DiskUsage}
 * and a map of shard ids to shard sizes, see
 * <code>InternalClusterInfoService.shardIdentifierFromRouting(String)</code>
 * for the key used in the shardSizes map
 *
 * @opensearch.internal
 */
public class ClusterInfo implements ToXContentFragment, Writeable {
    private final Map<String, DiskUsage> leastAvailableSpaceUsage;
    private final Map<String, DiskUsage> mostAvailableSpaceUsage;
    final Map<String, Long> shardSizes;  // pkg-private for testing only
    public static final ClusterInfo EMPTY = new ClusterInfo();
    final Map<ShardRouting, String> routingToDataPath;
    final Map<NodeAndPath, ReservedSpace> reservedSpace;
    final Map<String, FileCacheStats> nodeFileCacheStats;

    protected ClusterInfo() {
        this(Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of());
    }

    /**
     * Creates a new ClusterInfo instance.
     *
     * @param leastAvailableSpaceUsage a node id to disk usage mapping for the path that has the least available space on the node.
     * @param mostAvailableSpaceUsage  a node id to disk usage mapping for the path that has the most available space on the node.
     * @param shardSizes a shardkey to size in bytes mapping per shard.
     * @param routingToDataPath the shard routing to datapath mapping
     * @param reservedSpace reserved space per shard broken down by node and data path
     * @see #shardIdentifierFromRouting
     */
    public ClusterInfo(
        final Map<String, DiskUsage> leastAvailableSpaceUsage,
        final Map<String, DiskUsage> mostAvailableSpaceUsage,
        final Map<String, Long> shardSizes,
        final Map<ShardRouting, String> routingToDataPath,
        final Map<NodeAndPath, ReservedSpace> reservedSpace,
        final Map<String, FileCacheStats> nodeFileCacheStats
    ) {
        this.leastAvailableSpaceUsage = leastAvailableSpaceUsage;
        this.shardSizes = shardSizes;
        this.mostAvailableSpaceUsage = mostAvailableSpaceUsage;
        this.routingToDataPath = routingToDataPath;
        this.reservedSpace = reservedSpace;
        this.nodeFileCacheStats = nodeFileCacheStats;
    }

    public ClusterInfo(StreamInput in) throws IOException {
        Map<String, DiskUsage> leastMap = in.readMap(StreamInput::readString, DiskUsage::new);
        Map<String, DiskUsage> mostMap = in.readMap(StreamInput::readString, DiskUsage::new);
        Map<String, Long> sizeMap = in.readMap(StreamInput::readString, StreamInput::readLong);
        Map<ShardRouting, String> routingMap = in.readMap(ShardRouting::new, StreamInput::readString);
        Map<NodeAndPath, ReservedSpace> reservedSpaceMap;
        reservedSpaceMap = in.readMap(NodeAndPath::new, ReservedSpace::new);

        this.leastAvailableSpaceUsage = Collections.unmodifiableMap(leastMap);
        this.mostAvailableSpaceUsage = Collections.unmodifiableMap(mostMap);
        this.shardSizes = Collections.unmodifiableMap(sizeMap);
        this.routingToDataPath = Collections.unmodifiableMap(routingMap);
        this.reservedSpace = Collections.unmodifiableMap(reservedSpaceMap);
        if (in.getVersion().onOrAfter(Version.V_2_10_0)) {
            this.nodeFileCacheStats = in.readMap(StreamInput::readString, FileCacheStats::new);
        } else {
            this.nodeFileCacheStats = Map.of();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.leastAvailableSpaceUsage, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        out.writeMap(this.mostAvailableSpaceUsage, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        out.writeMap(this.shardSizes, StreamOutput::writeString, (o, v) -> out.writeLong(v == null ? -1 : v));
        out.writeMap(this.routingToDataPath, (o, k) -> k.writeTo(o), StreamOutput::writeString);
        out.writeMap(this.reservedSpace, (o, v) -> v.writeTo(o), (o, v) -> v.writeTo(o));
        if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
            out.writeMap(this.nodeFileCacheStats, StreamOutput::writeString, (o, v) -> v.writeTo(o));
        }
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("nodes");
        {
            for (Map.Entry<String, DiskUsage> c : this.leastAvailableSpaceUsage.entrySet()) {
                builder.startObject(c.getKey());
                { // node
                    builder.field("node_name", c.getValue().getNodeName());
                    builder.startObject("least_available");
                    {
                        c.getValue().toShortXContent(builder);
                    }
                    builder.endObject(); // end "least_available"
                    builder.startObject("most_available");
                    {
                        DiskUsage most = this.mostAvailableSpaceUsage.get(c.getKey());
                        if (most != null) {
                            most.toShortXContent(builder);
                        }
                    }
                    builder.endObject(); // end "most_available"
                }
                builder.endObject(); // end $nodename
            }
        }
        builder.endObject(); // end "nodes"
        builder.startObject("shard_sizes");
        {
            for (Map.Entry<String, Long> c : this.shardSizes.entrySet()) {
                builder.humanReadableField(c.getKey() + "_bytes", c.getKey(), new ByteSizeValue(c.getValue()));
            }
        }
        builder.endObject(); // end "shard_sizes"
        builder.startObject("shard_paths");
        {
            for (Map.Entry<ShardRouting, String> c : this.routingToDataPath.entrySet()) {
                builder.field(c.getKey().toString(), c.getValue());
            }
        }
        builder.endObject(); // end "shard_paths"
        builder.startArray("reserved_sizes");
        {
            for (Map.Entry<NodeAndPath, ReservedSpace> c : this.reservedSpace.entrySet()) {
                builder.startObject();
                {
                    builder.field("node_id", c.getKey().nodeId);
                    builder.field("path", c.getKey().path);
                    c.getValue().toXContent(builder, params);
                }
                builder.endObject(); // NodeAndPath
            }
        }
        builder.endArray(); // end "reserved_sizes"
        return builder;
    }

    /**
     * Returns a node id to disk usage mapping for the path that has the least available space on the node.
     * Note that this does not take account of reserved space: there may be another path with less available _and unreserved_ space.
     */
    public Map<String, DiskUsage> getNodeLeastAvailableDiskUsages() {
        return Collections.unmodifiableMap(this.leastAvailableSpaceUsage);
    }

    /**
     * Returns a node id to disk usage mapping for the path that has the most available space on the node.
     * Note that this does not take account of reserved space: there may be another path with more available _and unreserved_ space.
     */
    public Map<String, DiskUsage> getNodeMostAvailableDiskUsages() {
        return Collections.unmodifiableMap(this.mostAvailableSpaceUsage);
    }

    /**
     * Returns a node id to file cache stats mapping for the nodes that have search roles assigned to it.
     */
    public Map<String, FileCacheStats> getNodeFileCacheStats() {
        return Collections.unmodifiableMap(this.nodeFileCacheStats);
    }

    /**
     * Returns the shard size for the given shard routing or <code>null</code> it that metric is not available.
     */
    public Long getShardSize(ShardRouting shardRouting) {
        return shardSizes.get(shardIdentifierFromRouting(shardRouting));
    }

    /**
     * Returns the nodes absolute data-path the given shard is allocated on or <code>null</code> if the information is not available.
     */
    public String getDataPath(ShardRouting shardRouting) {
        return routingToDataPath.get(shardRouting);
    }

    /**
     * Returns the shard size for the given shard routing or <code>defaultValue</code> it that metric is not available.
     */
    public long getShardSize(ShardRouting shardRouting, long defaultValue) {
        Long shardSize = getShardSize(shardRouting);
        return shardSize == null ? defaultValue : shardSize;
    }

    /**
     * Returns the reserved space for each shard on the given node/path pair
     */
    public ReservedSpace getReservedSpace(String nodeId, String dataPath) {
        final ReservedSpace result = reservedSpace.get(new NodeAndPath(nodeId, dataPath));
        return result == null ? ReservedSpace.EMPTY : result;
    }

    /**
     * Method that incorporates the ShardId for the shard into a string that
     * includes a 'p' or 'r' depending on whether the shard is a primary.
     */
    static String shardIdentifierFromRouting(ShardRouting shardRouting) {
        return shardRouting.shardId().toString() + "[" + (shardRouting.primary() ? "p" : "r") + "]";
    }

    /**
     * Represents a data path on a node
     *
     * @opensearch.internal
     */
    public static class NodeAndPath implements Writeable {
        public final String nodeId;
        public final String path;

        public NodeAndPath(String nodeId, String path) {
            this.nodeId = Objects.requireNonNull(nodeId);
            this.path = Objects.requireNonNull(path);
        }

        public NodeAndPath(StreamInput in) throws IOException {
            this.nodeId = in.readString();
            this.path = in.readString();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeAndPath that = (NodeAndPath) o;
            return nodeId.equals(that.nodeId) && path.equals(that.path);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, path);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(nodeId);
            out.writeString(path);
        }
    }

    /**
     * Represents the total amount of "reserved" space on a particular data path, together with the set of shards considered.
     *
     * @opensearch.internal
     */
    public static class ReservedSpace implements Writeable {

        public static final ReservedSpace EMPTY = new ReservedSpace(0, new HashSet<>());

        private final long total;
        private final Set<ShardId> shardIds;

        private ReservedSpace(long total, Set<ShardId> shardIds) {
            this.total = total;
            this.shardIds = Collections.unmodifiableSet(shardIds);
        }

        ReservedSpace(StreamInput in) throws IOException {
            total = in.readVLong();
            final int shardIdCount = in.readVInt();
            Set<ShardId> shardIds = new HashSet<>(shardIdCount);
            for (int i = 0; i < shardIdCount; i++) {
                shardIds.add(new ShardId(in));
            }
            this.shardIds = Collections.unmodifiableSet(shardIds);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(total);
            out.writeVInt(shardIds.size());
            for (final ShardId shardIdCursor : shardIds) {
                shardIdCursor.writeTo(out);
            }
        }

        public long getTotal() {
            return total;
        }

        public boolean containsShardId(ShardId shardId) {
            return shardIds.contains(shardId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ReservedSpace that = (ReservedSpace) o;
            return total == that.total && shardIds.equals(that.shardIds);
        }

        @Override
        public int hashCode() {
            return Objects.hash(total, shardIds);
        }

        void toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("total", total);
            builder.startArray("shards");
            {
                for (final ShardId shardIdCursor : shardIds) {
                    shardIdCursor.toXContent(builder, params);
                }
            }
            builder.endArray(); // end "shards"
        }

        /**
         * Builder for Reserved Space.
         *
         * @opensearch.internal
         */
        public static class Builder {
            private long total;
            private Set<ShardId> shardIds = new HashSet<>();

            public ReservedSpace build() {
                assert shardIds != null : "already built";
                final ReservedSpace reservedSpace = new ReservedSpace(total, shardIds);
                shardIds = null;
                return reservedSpace;
            }

            public Builder add(ShardId shardId, long reservedBytes) {
                assert shardIds != null : "already built";
                assert reservedBytes >= 0 : reservedBytes;
                shardIds.add(shardId);
                total += reservedBytes;
                return this;
            }
        }
    }

}
