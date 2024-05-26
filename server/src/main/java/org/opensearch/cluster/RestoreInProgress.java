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

import org.elasticsearch.snapshots.SnapshotId;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterState.Custom;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParserUtils;
import org.opensearch.snapshots.Snapshot;
import org.opensearch.snapshots.SnapshotId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Meta data about restore processes that are currently executing
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class RestoreInProgress extends AbstractNamedDiffable<Custom> implements Custom, Iterable<RestoreInProgress.Entry> {

    /**
     * Fallback UUID used for restore operations that were started before v6.6 and don't have a uuid in the cluster state.
     */
    public static final String BWC_UUID = new UUID(0, 0).toString();

    public static final String TYPE = "restore";

    public static final RestoreInProgress EMPTY = new RestoreInProgress(Map.of());

    private final Map<String, Entry> entries;

    /**
     * Constructs new restore metadata
     *
     * @param entries map of currently running restore processes keyed by their restore uuid
     */
    private RestoreInProgress(final Map<String, Entry> entries) {
        this.entries = Collections.unmodifiableMap(entries);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        return entries.equals(((RestoreInProgress) o).entries);
    }

    @Override
    public int hashCode() {
        return entries.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("RestoreInProgress[");
        entries.forEach((s, entry) -> builder.append("{").append(s).append("}{").append(entry.snapshot).append("},"));
        builder.setCharAt(builder.length() - 1, ']');
        return builder.toString();
    }

    public Entry get(String restoreUUID) {
        return entries.get(restoreUUID);
    }

    public boolean isEmpty() {
        return entries.isEmpty();
    }

    @Override
    public Iterator<Entry> iterator() {
        return entries.values().iterator();
    }

    /**
     * Builder of the restore.
     *
     * @opensearch.internal
     */
    public static final class Builder {

        private final Map<String, Entry> entries = new HashMap<>();

        public Builder() {}

        public Builder(RestoreInProgress restoreInProgress) {
            entries.putAll(restoreInProgress.entries);
        }

        public Builder add(Entry entry) {
            entries.put(entry.uuid, entry);
            return this;
        }

        public RestoreInProgress build() {
            return entries.isEmpty() ? EMPTY : new RestoreInProgress(entries);
        }
    }

    /**
     * Restore metadata
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Entry implements ToXContentFragment {
        private final String uuid;
        private final State state;
        private final Snapshot snapshot;
        private final Map<ShardId, ShardRestoreStatus> shards;
        private final List<String> indices;

        /**
         * Creates new restore metadata
         *
         * @param uuid       uuid of the restore
         * @param snapshot   snapshot
         * @param state      current state of the restore process
         * @param indices    list of indices being restored
         * @param shards     map of shards being restored to their current restore status
         */
        public Entry(String uuid, Snapshot snapshot, State state, List<String> indices, final Map<ShardId, ShardRestoreStatus> shards) {
            this.snapshot = Objects.requireNonNull(snapshot);
            this.state = Objects.requireNonNull(state);
            this.indices = Objects.requireNonNull(indices);
            if (shards == null) {
                this.shards = Map.of();
            } else {
                this.shards = Collections.unmodifiableMap(shards);
            }
            this.uuid = Objects.requireNonNull(uuid);
        }

        /**
         * Returns restore uuid
         * @return restore uuid
         */
        public String uuid() {
            return uuid;
        }

        /**
         * Returns snapshot
         *
         * @return snapshot
         */
        public Snapshot snapshot() {
            return this.snapshot;
        }

        /**
         * Returns list of shards that being restore and their status
         *
         * @return map of shard id to shard restore status
         */
        public Map<ShardId, ShardRestoreStatus> shards() {
            return this.shards;
        }

        /**
         * Returns current restore state
         *
         * @return restore state
         */
        public State state() {
            return state;
        }

        /**
         * Returns list of indices
         *
         * @return list of indices
         */
        public List<String> indices() {
            return indices;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Entry entry = (Entry) o;
            return uuid.equals(entry.uuid)
                && snapshot.equals(entry.snapshot)
                && state == entry.state
                && indices.equals(entry.indices)
                && shards.equals(entry.shards);
        }

        @Override
        public int hashCode() {
            return Objects.hash(uuid, snapshot, state, indices, shards);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            boolean isGatewayXContent = params.param(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API)
                .equals(Metadata.CONTEXT_MODE_GATEWAY);
            builder.startObject();
            builder.field("snapshot", snapshot().getSnapshotId().getName());
            builder.field("repository", snapshot().getRepository());
            builder.field("state", state());
            if (isGatewayXContent) {
                builder.field("snapshot_uuid", snapshot().getSnapshotId().getUUID());
                builder.field("uuid", uuid());
            }
            builder.startArray("indices");
            {
                for (String index : indices()) {
                    builder.value(index);
                }
            }
            builder.endArray();
            builder.startArray("shards");
            {
                for (final Map.Entry<ShardId, ShardRestoreStatus> shardEntry : shards.entrySet()) {
                    ShardId shardId = shardEntry.getKey();
                    ShardRestoreStatus status = shardEntry.getValue();
                    builder.startObject();
                    {
                        builder.field("index", shardId.getIndex());
                        builder.field("shard", shardId.getId());
                        builder.field("state", status.state());
                        if (isGatewayXContent) {
                            builder.field("index_uuid", shardId.getIndex().getUUID());
                            if (status.nodeId() != null) builder.field("node_id", status.nodeId());
                            if (status.reason() != null) builder.field("reason", status.reason());
                        }
                    }
                    builder.endObject();
                }
            }

            builder.endArray();
            builder.endObject();
            return builder;
        }

        public static Entry fromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() == null) {
                parser.nextToken();
            }
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
            String snapshotName = null;
            String snapshotRepository = null;
            String snapshotUUID = null;
            int state = -1;
            String uuid = null;
            List<String> indices = new ArrayList<>();
            Map<ShardId, ShardRestoreStatus> shards = new HashMap<>();
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                String currentFieldName = parser.currentName();
                parser.nextToken();
                switch (currentFieldName) {
                    case "snapshot":
                        snapshotName = parser.text();
                        break;
                    case "repository":
                        snapshotRepository = parser.text();
                        break;
                    case "state":
                        state = parser.intValue();
                        break;
                    case "snapshot_uuid":
                        snapshotUUID = parser.text();
                        break;
                    case "uuid":
                        uuid = parser.text();
                        break;
                    case "indices":
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            indices.add(parser.text());
                        }
                        break;
                    case "shards":
                        ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.currentToken(), parser);
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            String indexName = null;
                            String indexUUID = null;
                            int shardId = -1;
                            int restoreState = -1;
                            String nodeId = null;
                            String reason = null;
                            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                                ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                                String currentShardFieldName = parser.currentName();
                                parser.nextToken();
                                switch (currentShardFieldName) {
                                    case "index":
                                        indexName = parser.text();
                                        break;
                                    case "shard":
                                        shardId = parser.intValue();
                                        break;
                                    case "state":
                                        restoreState = parser.intValue();
                                        break;
                                    case "index_uuid":
                                        indexUUID = parser.text();
                                        break;
                                    case "node_id":
                                        nodeId = parser.text();
                                        break;
                                    case "reason":
                                        reason = parser.text();
                                        break;
                                    default:
                                        throw new IllegalArgumentException("unknown field [" + currentFieldName + "]");
                                }
                            }
                            shards.put(new ShardId(indexName, indexUUID, shardId), new ShardRestoreStatus(nodeId, State.fromValue((byte) restoreState), reason));
                        }
                        break;
                    default:
                        throw new IllegalArgumentException("unknown field [" + currentFieldName + "]");
                }
            }
            return new Entry(uuid, new Snapshot(snapshotRepository, new SnapshotId(snapshotName, snapshotUUID)), State.fromValue((byte) state), indices, shards);
        }
    }

    /**
     * Represents status of a restored shard
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class ShardRestoreStatus implements Writeable {
        private State state;
        private String nodeId;
        private String reason;

        private ShardRestoreStatus() {}

        /**
         * Constructs a new shard restore status in initializing state on the given node
         *
         * @param nodeId node id
         */
        public ShardRestoreStatus(String nodeId) {
            this(nodeId, State.INIT);
        }

        /**
         * Constructs a new shard restore status in with specified state on the given node
         *
         * @param nodeId node id
         * @param state  restore state
         */
        public ShardRestoreStatus(String nodeId, State state) {
            this(nodeId, state, null);
        }

        /**
         * Constructs a new shard restore status in with specified state on the given node with specified failure reason
         *
         * @param nodeId node id
         * @param state  restore state
         * @param reason failure reason
         */
        public ShardRestoreStatus(String nodeId, State state, String reason) {
            this.nodeId = nodeId;
            this.state = state;
            this.reason = reason;
        }

        /**
         * Returns current state
         *
         * @return current state
         */
        public State state() {
            return state;
        }

        /**
         * Returns node id of the node where shared is getting restored
         *
         * @return node id
         */
        public String nodeId() {
            return nodeId;
        }

        /**
         * Returns failure reason
         *
         * @return failure reason
         */
        public String reason() {
            return reason;
        }

        /**
         * Reads restore status from stream input
         *
         * @param in stream input
         * @return restore status
         */
        public static ShardRestoreStatus readShardRestoreStatus(StreamInput in) throws IOException {
            ShardRestoreStatus shardSnapshotStatus = new ShardRestoreStatus();
            shardSnapshotStatus.readFrom(in);
            return shardSnapshotStatus;
        }

        /**
         * Reads restore status from stream input
         *
         * @param in stream input
         */
        public void readFrom(StreamInput in) throws IOException {
            nodeId = in.readOptionalString();
            state = State.fromValue(in.readByte());
            reason = in.readOptionalString();
        }

        /**
         * Writes restore status to stream output
         *
         * @param out stream input
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(nodeId);
            out.writeByte(state.value);
            out.writeOptionalString(reason);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ShardRestoreStatus status = (ShardRestoreStatus) o;
            return state == status.state && Objects.equals(nodeId, status.nodeId) && Objects.equals(reason, status.reason);
        }

        @Override
        public int hashCode() {
            return Objects.hash(state, nodeId, reason);
        }
    }

    /**
     * Shard restore process state
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum State {
        /**
         * Initializing state
         */
        INIT((byte) 0),
        /**
         * Started state
         */
        STARTED((byte) 1),
        /**
         * Restore finished successfully
         */
        SUCCESS((byte) 2),
        /**
         * Restore failed
         */
        FAILURE((byte) 3);

        private final byte value;

        /**
         * Constructs new state
         *
         * @param value state code
         */
        State(byte value) {
            this.value = value;
        }

        /**
         * Returns state code
         *
         * @return state code
         */
        public byte value() {
            return value;
        }

        /**
         * Returns true if restore process completed (either successfully or with failure)
         *
         * @return true if restore process completed
         */
        public boolean completed() {
            return this == SUCCESS || this == FAILURE;
        }

        /**
         * Returns state corresponding to state code
         *
         * @param value stat code
         * @return state
         */
        public static State fromValue(byte value) {
            switch (value) {
                case 0:
                    return INIT;
                case 1:
                    return STARTED;
                case 2:
                    return SUCCESS;
                case 3:
                    return FAILURE;
                default:
                    throw new IllegalArgumentException("No snapshot state for value [" + value + "]");
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    public static NamedDiff<Custom> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(Custom.class, TYPE, in);
    }

    public RestoreInProgress(StreamInput in) throws IOException {
        int count = in.readVInt();
        final Map<String, Entry> entriesBuilder = new HashMap<>(count);
        for (int i = 0; i < count; i++) {
            final String uuid;
            uuid = in.readString();
            Snapshot snapshot = new Snapshot(in);
            State state = State.fromValue(in.readByte());
            List<String> indexBuilder = in.readStringList();
            entriesBuilder.put(
                uuid,
                new Entry(
                    uuid,
                    snapshot,
                    state,
                    Collections.unmodifiableList(indexBuilder),
                    in.readMap(ShardId::new, ShardRestoreStatus::readShardRestoreStatus)
                )
            );
        }
        this.entries = Collections.unmodifiableMap(entriesBuilder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(entries.size());
        for (final Entry entry : entries.values()) {
            out.writeString(entry.uuid);
            entry.snapshot().writeTo(out);
            out.writeByte(entry.state().value());
            out.writeStringCollection(entry.indices);
            out.writeMap(entry.shards, (o, shardId) -> shardId.writeTo(o), (o, status) -> status.writeTo(o));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startArray("snapshots");
        for (final Entry entry : entries.values()) {
            toXContent(entry, builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endArray();
        return builder;
    }

    public static RestoreInProgress fromXContent(XContentParser parser) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        final Map<String, Entry> entries = new HashMap<>();
        XContentParserUtils.ensureFieldName(parser, parser.currentToken(), "snapshots");
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            final Entry entry = Entry.fromXContent(parser);
            entries.put(entry.uuid, entry);
        }
        return new RestoreInProgress(entries);
    }

    /**
     * Serializes single restore operation
     *
     * @param entry   restore operation metadata
     * @param builder XContent builder
     * @param params
     */
    public void toXContent(Entry entry, XContentBuilder builder, ToXContent.Params params) throws IOException {
        entry.toXContent(builder, params);
    }
}
























