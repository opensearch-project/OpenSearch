/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Per-shard information returned in a {@link SearchResponse} when the {@code shard_info=true}
 * request parameter is set. It lists the shard copies the search ran against, grouped by outcome:
 * {@code successful} entries completed every search phase, {@code skipped} entries were excluded
 * by the can-match pre-filter without contacting any node, and {@code failed} entries did not
 * complete.
 *
 * <p>When every searched cluster is described, the groups reconcile with the {@code _shards} counters:
 * the number of successful plus skipped entries equals {@code _shards.successful}, and the number of
 * failed entries equals {@code _shards.failed}. Two things make the arrays shorter than the counters
 * rather than equal to them: a failure that carries no shard identity at all cannot be listed, and is
 * visible only in {@code _shards.failures}; and the shards of a cluster that is not described, as
 * below, are still counted.
 *
 * <p>Each {@link Entry} always carries the index name and shard number; the node id, node name,
 * primary flag, shard state, and cluster alias are included only when they are known to the
 * coordinating node and are omitted otherwise. Entries are never invented from cluster-state
 * lookups: skipped entries carry no node attribution because no node executed anything, and shards
 * targeted through plain node ids, which is how point-in-time readers are resolved, omit the
 * primary flag and shard state because no shard routing is available for them. The node name is
 * resolved against the coordinating cluster's own node list, so it is absent for shards this node
 * coordinates on a remote cluster's behalf.
 *
 * <p>In a cross-cluster search a remote cluster is described only when it understands the feature, which the
 * two round-trip modes establish differently. With minimized round-trips, which is the default, each remote
 * builds and stamps its own entries, so a remote older than 3.8 never receives the request flag and
 * contributes nothing, while a remote part way through an upgrade describes all of its shards, since only
 * the node coordinating there needs to understand the request. Without minimized round-trips this node
 * resolves the remote shards itself, and
 * describes a remote only when every node of it that serves the search is on 3.8 or later, so a remote part
 * way through an upgrade is not described at all. Round-trips are not minimized when
 * {@code ccs_minimize_roundtrips=false} is requested, and are forced not to be for scroll, point-in-time,
 * {@code dfs_query_then_fetch}, and collapse with inner hits. Either way the shards of a cluster that is not
 * described are searched as usual but omitted from the arrays, with nothing in the response identifying the
 * omission, and if no cluster qualifies the section is absent entirely.
 *
 * <p>Entries are kept sorted by cluster alias, index, shard, and node id, so the rendered arrays
 * are deterministic for repeated identical requests, including cross-cluster searches whose
 * per-cluster responses arrive in arbitrary order.
 *
 * <p>The XContent representation is a {@code shard_info} object nested inside the {@code _shards} block of
 * the response. It describes the search that produced that response, so a scrolling search carries it on the
 * initial response only and not on the continuation pages fetched through {@code _search/scroll}.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.8.0")
public final class SearchShardInfo implements Writeable, ToXContentFragment {

    public static final String SHARD_INFO_FIELD = "shard_info";
    private static final String SUCCESSFUL_FIELD = "successful";
    private static final String SKIPPED_FIELD = "skipped";
    private static final String FAILED_FIELD = "failed";

    public static final SearchShardInfo EMPTY = new SearchShardInfo(
        Collections.emptyList(),
        Collections.emptyList(),
        Collections.emptyList()
    );

    private final List<Entry> successful;
    private final List<Entry> skipped;
    private final List<Entry> failed;

    public SearchShardInfo(List<Entry> successful, List<Entry> skipped, List<Entry> failed) {
        this.successful = sortedCopy(successful);
        this.skipped = sortedCopy(skipped);
        this.failed = sortedCopy(failed);
    }

    public SearchShardInfo(StreamInput in) throws IOException {
        this.successful = sortedCopy(in.readList(Entry::new));
        this.skipped = sortedCopy(in.readList(Entry::new));
        this.failed = sortedCopy(in.readList(Entry::new));
    }

    private static List<Entry> sortedCopy(List<Entry> entries) {
        if (entries.isEmpty()) {
            return Collections.emptyList();
        }
        List<Entry> copy = new ArrayList<>(entries);
        copy.sort(Entry.COMPARATOR);
        return Collections.unmodifiableList(copy);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(successful);
        out.writeList(skipped);
        out.writeList(failed);
    }

    public List<Entry> getSuccessful() {
        return successful;
    }

    public List<Entry> getSkipped() {
        return skipped;
    }

    public List<Entry> getFailed() {
        return failed;
    }

    public boolean isEmpty() {
        return successful.isEmpty() && skipped.isEmpty() && failed.isEmpty();
    }

    /**
     * Combines the given instances into one, ignoring {@code null} elements. Used to combine
     * per-cluster shard information when cross-cluster search responses are merged. The result is
     * independent of the order of the input list because entries are kept in their canonical sort
     * order.
     */
    public static SearchShardInfo merge(List<SearchShardInfo> infos) {
        List<Entry> successful = new ArrayList<>();
        List<Entry> skipped = new ArrayList<>();
        List<Entry> failed = new ArrayList<>();
        for (SearchShardInfo info : infos) {
            if (info == null) {
                continue;
            }
            successful.addAll(info.successful);
            skipped.addAll(info.skipped);
            failed.addAll(info.failed);
        }
        if (successful.isEmpty() && skipped.isEmpty() && failed.isEmpty()) {
            return EMPTY;
        }
        return new SearchShardInfo(successful, skipped, failed);
    }

    /**
     * Returns a copy in which every entry that does not already carry a cluster alias is stamped
     * with the given alias. Returns this instance unchanged when the alias is {@code null} or
     * empty (the local cluster group key) or when there is nothing to stamp. Used by the
     * coordinating node to attribute the entries of a remote cluster's response to that cluster.
     */
    public SearchShardInfo withClusterAlias(@Nullable String clusterAlias) {
        if (clusterAlias == null || clusterAlias.isEmpty() || isEmpty()) {
            return this;
        }
        return new SearchShardInfo(stamped(successful, clusterAlias), stamped(skipped, clusterAlias), stamped(failed, clusterAlias));
    }

    private static List<Entry> stamped(List<Entry> entries, String clusterAlias) {
        List<Entry> result = new ArrayList<>(entries.size());
        for (Entry entry : entries) {
            result.add(entry.withCluster(clusterAlias));
        }
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(SHARD_INFO_FIELD);
        writeArray(builder, params, SUCCESSFUL_FIELD, successful);
        writeArray(builder, params, SKIPPED_FIELD, skipped);
        writeArray(builder, params, FAILED_FIELD, failed);
        builder.endObject();
        return builder;
    }

    private static void writeArray(XContentBuilder builder, Params params, String fieldName, List<Entry> entries) throws IOException {
        builder.startArray(fieldName);
        for (Entry entry : entries) {
            entry.toXContent(builder, params);
        }
        builder.endArray();
    }

    /**
     * Parses a {@code shard_info} object. The parser must be positioned on the
     * {@link XContentParser.Token#START_OBJECT} of the object's value.
     */
    public static SearchShardInfo fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        List<Entry> successful = new ArrayList<>();
        List<Entry> skipped = new ArrayList<>();
        List<Entry> failed = new ArrayList<>();
        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                List<Entry> target = switch (currentFieldName) {
                    case SUCCESSFUL_FIELD -> successful;
                    case SKIPPED_FIELD -> skipped;
                    case FAILED_FIELD -> failed;
                    case null, default -> null;
                };
                if (target == null) {
                    parser.skipChildren();
                } else {
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        target.add(Entry.fromXContent(parser));
                    }
                }
            } else {
                parser.skipChildren();
            }
        }
        return new SearchShardInfo(successful, skipped, failed);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SearchShardInfo that = (SearchShardInfo) o;
        return successful.equals(that.successful) && skipped.equals(that.skipped) && failed.equals(that.failed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(successful, skipped, failed);
    }

    @Override
    public String toString() {
        return "SearchShardInfo{successful=" + successful + ", skipped=" + skipped + ", failed=" + failed + "}";
    }

    /**
     * A single shard copy that participated in (or was skipped by) a search. The index name and
     * shard number are always present; all other fields are optional and omitted when unknown.
     *
     * @opensearch.api
     */
    @PublicApi(since = "3.8.0")
    public static final class Entry implements Writeable, ToXContentObject {

        static final Comparator<Entry> COMPARATOR = Comparator.comparing(
            Entry::getCluster,
            Comparator.nullsFirst(Comparator.naturalOrder())
        )
            .thenComparing(Entry::getIndex)
            .thenComparingInt(Entry::getShard)
            .thenComparing(Entry::getNodeId, Comparator.nullsFirst(Comparator.naturalOrder()));

        private static final String INDEX_FIELD = "index";
        private static final String SHARD_FIELD = "shard";
        private static final String NODE_ID_FIELD = "node_id";
        private static final String NODE_NAME_FIELD = "node_name";
        private static final String PRIMARY_FIELD = "primary";
        private static final String STATE_FIELD = "state";
        private static final String CLUSTER_FIELD = "cluster";

        private final String index;
        private final int shard;
        @Nullable
        private final String nodeId;
        @Nullable
        private final String nodeName;
        @Nullable
        private final Boolean primary;
        @Nullable
        private final String state;
        @Nullable
        private final String cluster;

        private Entry(
            String index,
            int shard,
            @Nullable String nodeId,
            @Nullable String nodeName,
            @Nullable Boolean primary,
            @Nullable String state,
            @Nullable String cluster
        ) {
            this.index = Objects.requireNonNull(index, "index must not be null");
            if (shard < 0) {
                throw new IllegalArgumentException("shard must be non-negative but was [" + shard + "]");
            }
            this.shard = shard;
            this.nodeId = nodeId;
            this.nodeName = nodeName;
            this.primary = primary;
            this.state = state;
            this.cluster = cluster;
        }

        private Entry(StreamInput in) throws IOException {
            this.index = in.readString();
            this.shard = in.readVInt();
            this.nodeId = in.readOptionalString();
            this.nodeName = in.readOptionalString();
            this.primary = in.readOptionalBoolean();
            this.state = in.readOptionalString();
            this.cluster = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeVInt(shard);
            out.writeOptionalString(nodeId);
            out.writeOptionalString(nodeName);
            out.writeOptionalBoolean(primary);
            out.writeOptionalString(state);
            out.writeOptionalString(cluster);
        }

        public String getIndex() {
            return index;
        }

        public int getShard() {
            return shard;
        }

        /**
         * The id of the node the shard was executed on, or {@code null} when no node was involved
         * (skipped shards) or the node is unknown.
         */
        @Nullable
        public String getNodeId() {
            return nodeId;
        }

        /**
         * The name of the node the shard was executed on, or {@code null} when it cannot be
         * resolved by the coordinating node.
         */
        @Nullable
        public String getNodeName() {
            return nodeName;
        }

        /**
         * Whether the executed shard copy was the primary, or {@code null} when unknown.
         */
        @Nullable
        public Boolean getPrimary() {
            return primary;
        }

        /**
         * The routing state name of the executed shard copy at the time it was selected, or
         * {@code null} when unknown.
         */
        @Nullable
        public String getState() {
            return state;
        }

        /**
         * The alias of the remote cluster the shard belongs to, or {@code null} for local shards.
         */
        @Nullable
        public String getCluster() {
            return cluster;
        }

        Entry withCluster(String clusterAlias) {
            if (this.cluster != null) {
                return this;
            }
            return new Entry(index, shard, nodeId, nodeName, primary, state, clusterAlias);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDEX_FIELD, index);
            builder.field(SHARD_FIELD, shard);
            if (nodeId != null) {
                builder.field(NODE_ID_FIELD, nodeId);
            }
            if (nodeName != null) {
                builder.field(NODE_NAME_FIELD, nodeName);
            }
            if (primary != null) {
                builder.field(PRIMARY_FIELD, primary.booleanValue());
            }
            if (state != null) {
                builder.field(STATE_FIELD, state);
            }
            if (cluster != null) {
                builder.field(CLUSTER_FIELD, cluster);
            }
            builder.endObject();
            return builder;
        }

        /**
         * Parses a single entry. The parser must be positioned on the
         * {@link XContentParser.Token#START_OBJECT} of the entry. Unknown fields are skipped for
         * forward compatibility.
         */
        public static Entry fromXContent(XContentParser parser) throws IOException {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
            String index = null;
            Integer shard = null;
            String nodeId = null;
            String nodeName = null;
            Boolean primary = null;
            String state = null;
            String cluster = null;
            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token.isValue()) {
                    if (INDEX_FIELD.equals(currentFieldName)) {
                        index = parser.text();
                    } else if (SHARD_FIELD.equals(currentFieldName)) {
                        shard = parser.intValue();
                    } else if (NODE_ID_FIELD.equals(currentFieldName)) {
                        nodeId = parser.text();
                    } else if (NODE_NAME_FIELD.equals(currentFieldName)) {
                        nodeName = parser.text();
                    } else if (PRIMARY_FIELD.equals(currentFieldName)) {
                        primary = parser.booleanValue();
                    } else if (STATE_FIELD.equals(currentFieldName)) {
                        state = parser.text();
                    } else if (CLUSTER_FIELD.equals(currentFieldName)) {
                        cluster = parser.text();
                    }
                } else {
                    parser.skipChildren();
                }
            }
            if (index == null) {
                throw new ParsingException(parser.getTokenLocation(), "missing required field [" + INDEX_FIELD + "]");
            }
            if (shard == null) {
                throw new ParsingException(parser.getTokenLocation(), "missing required field [" + SHARD_FIELD + "]");
            }
            return new Entry(index, shard, nodeId, nodeName, primary, state, cluster);
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
            return shard == entry.shard
                && index.equals(entry.index)
                && Objects.equals(nodeId, entry.nodeId)
                && Objects.equals(nodeName, entry.nodeName)
                && Objects.equals(primary, entry.primary)
                && Objects.equals(state, entry.state)
                && Objects.equals(cluster, entry.cluster);
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, shard, nodeId, nodeName, primary, state, cluster);
        }

        @Override
        public String toString() {
            return "Entry{index="
                + index
                + ", shard="
                + shard
                + ", nodeId="
                + nodeId
                + ", nodeName="
                + nodeName
                + ", primary="
                + primary
                + ", state="
                + state
                + ", cluster="
                + cluster
                + "}";
        }

        /**
         * Builder for {@link Entry}. The index name and shard number are required; every other
         * field is optional.
         *
         * @opensearch.api
         */
        @PublicApi(since = "3.8.0")
        public static final class Builder {
            private final String index;
            private final int shard;
            private String nodeId;
            private String nodeName;
            private Boolean primary;
            private String state;
            private String cluster;

            public Builder(String index, int shard) {
                this.index = index;
                this.shard = shard;
            }

            public Builder nodeId(@Nullable String nodeId) {
                this.nodeId = nodeId;
                return this;
            }

            public Builder nodeName(@Nullable String nodeName) {
                this.nodeName = nodeName;
                return this;
            }

            public Builder primary(@Nullable Boolean primary) {
                this.primary = primary;
                return this;
            }

            public Builder state(@Nullable String state) {
                this.state = state;
                return this;
            }

            public Builder cluster(@Nullable String cluster) {
                this.cluster = cluster;
                return this;
            }

            public Entry build() {
                return new Entry(index, shard, nodeId, nodeName, primary, state, cluster);
            }
        }
    }
}
