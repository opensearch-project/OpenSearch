/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.NamedDiff;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.cluster.metadata.Metadata.ALL_CONTEXTS;

/**
 * This class holds the QueryGroupMetadata
 * sample schema
 * {
 *     "queryGroups": {
 *         "_id": {
 *             {@link QueryGroup}
 *         },
 *        ...
 *     }
 * }
 */
@ExperimentalApi
public class QueryGroupMetadata implements Metadata.Custom {
    public static final String TYPE = "queryGroup";
    private static final ParseField QUERY_GROUP_FIELD = new ParseField("queryGroups");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<QueryGroupMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "queryGroupParser",
        args -> new QueryGroupMetadata((Set<QueryGroup>) args[0])
    );

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> QueryGroup.fromXContent(p), QUERY_GROUP_FIELD);
    }

    private final Set<QueryGroup> queryGroups;

    public QueryGroupMetadata(Set<QueryGroup> queryGroups) {
        this.queryGroups = queryGroups;
    }

    public QueryGroupMetadata(StreamInput in) throws IOException {
        this.queryGroups = in.readSet(QueryGroup::new);
    }

    public Set<QueryGroup> queryGroups() {
        return this.queryGroups;
    }

    /**
     * Returns the name of the writeable object
     */
    @Override
    public String getWriteableName() {
        return TYPE;
    }

    /**
     * The minimal version of the recipient this object can be sent to
     */
    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_3_0_0;
    }

    /**
     * Write this into the {@linkplain StreamOutput}.
     *
     * @param out
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(queryGroups);
    }

    /**
     * @param builder
     * @param params
     * @return
     * @throws IOException
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(QUERY_GROUP_FIELD.getPreferredName(), queryGroups);
        return builder;
    }

    public static QueryGroupMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    /**
     * Returns serializable object representing differences between this and previousState
     *
     * @param previousState
     */
    @Override
    public Diff<Metadata.Custom> diff(final Metadata.Custom previousState) {
        return new QueryGroupMetadataDiff((QueryGroupMetadata) previousState, this);
    }

    /**
     * @param in
     * @return the metadata diff for {@link QueryGroupMetadata} objects
     * @throws IOException
     */
    public static NamedDiff<Metadata.Custom> readDiffFrom(StreamInput in) throws IOException {
        return new QueryGroupMetadataDiff(in);
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        return ALL_CONTEXTS;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryGroupMetadata that = (QueryGroupMetadata) o;
        return Objects.equals(queryGroups, that.queryGroups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryGroups);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    /**
     * QueryGroupMetadataDiff
     */
    static class QueryGroupMetadataDiff implements NamedDiff<Metadata.Custom> {
        final Diff<Map<String, QueryGroup>> dataStreamDiff;

        QueryGroupMetadataDiff(final QueryGroupMetadata before, final QueryGroupMetadata after) {
            dataStreamDiff = DiffableUtils.diff(
                toMap(before.queryGroups),
                toMap(after.queryGroups),
                DiffableUtils.getStringKeySerializer()
            );
        }

        QueryGroupMetadataDiff(final StreamInput in) throws IOException {
            this.dataStreamDiff = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                QueryGroup::new,
                QueryGroup::readDiff
            );
        }

        private Map<String, QueryGroup> toMap(Set<QueryGroup> queryGroups) {
            final Map<String, QueryGroup> queryGroupMap = new HashMap<>();

            queryGroups.forEach(queryGroup -> queryGroupMap.put(queryGroup.getName(), queryGroup));
            return queryGroupMap;
        }

        /**
         * Returns the name of the writeable object
         */
        @Override
        public String getWriteableName() {
            return TYPE;
        }

        /**
         * Write this into the {@linkplain StreamOutput}.
         *
         * @param out
         */
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            dataStreamDiff.writeTo(out);
        }

        /**
         * Applies difference to the specified part and returns the resulted part
         *
         * @param part
         */
        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new QueryGroupMetadata(new HashSet<>(dataStreamDiff.apply(toMap(((QueryGroupMetadata) part).queryGroups)).values()));
        }
    }
}
