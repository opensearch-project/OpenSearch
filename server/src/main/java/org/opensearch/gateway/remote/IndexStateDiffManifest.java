/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * Manifest tracking differences in index metadata between states
 *
 * @opensearch.internal
 */
public class IndexStateDiffManifest implements ToXContentFragment, Writeable {
    private static final String FROM_STATE_VERSION_FIELD = "from_state_version";
    private static final String TO_STATE_VERSION_FIELD = "to_state_version";
    private static final String INDICES_UPDATED_FIELD = "indices_updated";
    private static final String INDICES_DELETED_FIELD = "indices_deleted";

    private final long fromStateVersion;
    private final long toStateVersion;
    private final List<String> indicesUpdated;
    private final List<String> indicesDeleted;

    public IndexStateDiffManifest(
        long fromStateVersion,
        long toStateVersion,
        List<String> indicesUpdated,
        List<String> indicesDeleted
    ) {
        this.fromStateVersion = fromStateVersion;
        this.toStateVersion = toStateVersion;
        this.indicesUpdated = Collections.unmodifiableList(indicesUpdated);
        this.indicesDeleted = Collections.unmodifiableList(indicesDeleted);
    }

    public IndexStateDiffManifest(StreamInput in) throws IOException {
        this.fromStateVersion = in.readVLong();
        this.toStateVersion = in.readVLong();
        this.indicesUpdated = in.readStringList();
        this.indicesDeleted = in.readStringList();
    }

    public long getFromStateVersion() {
        return fromStateVersion;
    }

    public long getToStateVersion() {
        return toStateVersion;
    }

    public List<String> getIndicesUpdated() {
        return indicesUpdated;
    }

    public List<String> getIndicesDeleted() {
        return indicesDeleted;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(fromStateVersion);
        out.writeVLong(toStateVersion);
        out.writeStringCollection(indicesUpdated);
        out.writeStringCollection(indicesDeleted);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FROM_STATE_VERSION_FIELD, fromStateVersion);
        builder.field(TO_STATE_VERSION_FIELD, toStateVersion);
        
        builder.startArray(INDICES_UPDATED_FIELD);
        for (String index : indicesUpdated) {
            builder.value(index);
        }
        builder.endArray();
        
        builder.startArray(INDICES_DELETED_FIELD);
        for (String index : indicesDeleted) {
            builder.value(index);
        }
        builder.endArray();
        
        return builder;
    }

    public static IndexStateDiffManifest fromXContent(XContentParser parser) throws IOException {
        long fromStateVersion = -1;
        long toStateVersion = -1;
        List<String> indicesUpdated = new ArrayList<>();
        List<String> indicesDeleted = new ArrayList<>();

        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);

        String currentFieldName = parser.currentName();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                switch (currentFieldName) {
                    case FROM_STATE_VERSION_FIELD:
                        fromStateVersion = parser.longValue();
                        break;
                    case TO_STATE_VERSION_FIELD:
                        toStateVersion = parser.longValue();
                        break;
                    default:
                        throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                switch (currentFieldName) {
                    case INDICES_UPDATED_FIELD:
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            indicesUpdated.add(parser.text());
                        }
                        break;
                    case INDICES_DELETED_FIELD:
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            indicesDeleted.add(parser.text());
                        }
                        break;
                    default:
                        throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                }
            }
        }

        return new IndexStateDiffManifest(fromStateVersion, toStateVersion, indicesUpdated, indicesDeleted);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexStateDiffManifest that = (IndexStateDiffManifest) o;
        return fromStateVersion == that.fromStateVersion
            && toStateVersion == that.toStateVersion
            && Objects.equals(indicesUpdated, that.indicesUpdated)
            && Objects.equals(indicesDeleted, that.indicesDeleted);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fromStateVersion, toStateVersion, indicesUpdated, indicesDeleted);
    }
}