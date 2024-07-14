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
import org.opensearch.core.ParseField;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
public class QueryGroupMetadata implements Metadata.Custom {
    public static final String TYPE = "queryGroups";
    private static final ParseField QUERY_GROUP_FIELD = new ParseField("queryGroups");

    private final Map<String, QueryGroup> queryGroups;

    public QueryGroupMetadata(Map<String, QueryGroup> queryGroups) {
        this.queryGroups = queryGroups;
    }

    public QueryGroupMetadata(StreamInput in) throws IOException {
        this.queryGroups = in.readMap(StreamInput::readString, QueryGroup::new);
    }

    public Map<String, QueryGroup> queryGroups() {
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(queryGroups, StreamOutput::writeString, (stream, val) -> val.writeTo(stream));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        for (Map.Entry<String, QueryGroup> entry : queryGroups.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        return builder;
    }

    public static QueryGroupMetadata fromXContent(XContentParser parser) throws IOException {
        Map<String, QueryGroup> queryGroupMap = new HashMap<>();

        if (parser.currentToken() == null) {
            parser.nextToken();
        }

        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException(
                "QueryGroupMetadata.fromXContent was expecting a { token but found : " + parser.currentToken()
            );
        }
        XContentParser.Token token = parser.currentToken();
        String fieldName = parser.currentName();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else {
                QueryGroup queryGroup = QueryGroup.fromXContent(parser);
                queryGroupMap.put(fieldName, queryGroup);
            }
        }

        return new QueryGroupMetadata(queryGroupMap);
    }

    @Override
    public Diff<Metadata.Custom> diff(final Metadata.Custom previousState) {
        return new QueryGroupMetadataDiff((QueryGroupMetadata) previousState, this);
    }

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
            dataStreamDiff = DiffableUtils.diff(before.queryGroups, after.queryGroups, DiffableUtils.getStringKeySerializer());
        }

        QueryGroupMetadataDiff(final StreamInput in) throws IOException {
            this.dataStreamDiff = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getStringKeySerializer(),
                QueryGroup::new,
                QueryGroup::readDiff
            );
        }

        /**
         * Returns the name of the writeable object
         */
        @Override
        public String getWriteableName() {
            return TYPE;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            dataStreamDiff.writeTo(out);
        }

        @Override
        public Metadata.Custom apply(Metadata.Custom part) {
            return new QueryGroupMetadata(new HashMap<>(dataStreamDiff.apply(((QueryGroupMetadata) part).queryGroups)));
        }
    }
}
