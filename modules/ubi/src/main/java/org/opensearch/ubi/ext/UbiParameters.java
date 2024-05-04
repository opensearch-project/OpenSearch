/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ubi.ext;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchExtBuilder;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * The UBI parameters available in the ext.
 */
public class UbiParameters implements Writeable, ToXContentObject {

    private static final ObjectParser<UbiParameters, Void> PARSER;
    private static final ParseField QUERY_ID = new ParseField("query_id");
    private static final ParseField USER_QUERY = new ParseField("user_query");
    private static final ParseField CLIENT_ID = new ParseField("client_id");
    private static final ParseField OBJECT_ID = new ParseField("object_id");

    static {
        PARSER = new ObjectParser<>(UbiParametersExtBuilder.UBI_PARAMETER_NAME, UbiParameters::new);
        PARSER.declareString(UbiParameters::setQueryId, QUERY_ID);
        PARSER.declareString(UbiParameters::setUserQuery, USER_QUERY);
        PARSER.declareString(UbiParameters::setClientId, CLIENT_ID);
        PARSER.declareString(UbiParameters::setObjectId, OBJECT_ID);
    }

    private String queryId;
    private String userQuery;
    private String clientId;
    private String objectId;

    /**
     * Get the {@link UbiParameters} from a {@link SearchRequest}.
     * @param request A {@link SearchRequest},
     * @return The UBI {@link UbiParameters parameters}.
     */
    public static UbiParameters getUbiParameters(final SearchRequest request) {

        UbiParametersExtBuilder builder = null;

        if (request.source() != null && request.source().ext() != null && !request.source().ext().isEmpty()) {
            final Optional<SearchExtBuilder> b = request.source()
                .ext()
                .stream()
                .filter(bldr -> UbiParametersExtBuilder.UBI_PARAMETER_NAME.equals(bldr.getWriteableName()))
                .findFirst();
            if (b.isPresent()) {
                builder = (UbiParametersExtBuilder) b.get();
            }
        }

        if (builder != null) {
            return builder.getParams();
        } else {
            return null;
        }

    }

    /**
     * Creates a new instance.
     */
    public UbiParameters() {}

    /**
     * Creates a new instance.
     * @param input The {@link StreamInput} to read parameters from.
     * @throws IOException Thrown if the parameters cannot be read.
     */
    public UbiParameters(StreamInput input) throws IOException {
        this.queryId = input.readString();
        this.userQuery = input.readOptionalString();
        this.clientId = input.readOptionalString();
        this.objectId = input.readOptionalString();
    }

    /**
     * Creates a new instance.
     * @param queryId The query ID.
     * @param userQuery The user-entered search query.
     * @param clientId The client ID.
     * @param objectId The object ID.
     */
    public UbiParameters(String queryId, String userQuery, String clientId, String objectId) {
        this.queryId = queryId;
        this.userQuery = userQuery;
        this.clientId = clientId;
        this.objectId = objectId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder xContentBuilder, Params params) throws IOException {
        return xContentBuilder.field(QUERY_ID.getPreferredName(), this.queryId)
            .field(USER_QUERY.getPreferredName(), this.userQuery)
            .field(CLIENT_ID.getPreferredName(), this.clientId)
            .field(OBJECT_ID.getPreferredName(), this.objectId);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(queryId);
        out.writeOptionalString(userQuery);
        out.writeOptionalString(clientId);
        out.writeOptionalString(objectId);
    }

    /**
     * Create the {@link UbiParameters} from a {@link XContentParser}.
     * @param parser An {@link XContentParser}.
     * @return The {@link UbiParameters}.
     * @throws IOException Thrown if the parameters cannot be read.
     */
    public static UbiParameters parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        UbiParameters other = (UbiParameters) o;
        return Objects.equals(this.queryId, other.getQueryId())
            && Objects.equals(this.userQuery, other.getUserQuery())
            && Objects.equals(this.clientId, other.getClientId())
            && Objects.equals(this.objectId, other.getObjectId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), this.queryId);
    }

    /**
     * Get the query ID.
     * @return The query ID, or a random UUID if the query ID is <code>null</code>.
     */
    public String getQueryId() {
        if (queryId == null || queryId.isEmpty()) {
            return UUID.randomUUID().toString();
        } else {
            return queryId;
        }
    }

    /**
     * Set the query ID.
     * @param queryId The query ID.
     */
    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    /**
     * Get the client ID.
     * @return The client ID.
     */
    public String getClientId() {
        return clientId;
    }

    /**
     * Set the client ID.
     * @param clientId The client ID.
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * Get the object ID.
     * @return The object ID.
     */
    public String getObjectId() {
        return objectId;
    }

    /**
     * Set the object ID.
     * @param objectId The object ID.
     */
    public void setObjectId(String objectId) {
        this.objectId = objectId;
    }

    /**
     * Get the user query.
     * @return The user query.
     */
    public String getUserQuery() {
        return userQuery;
    }

    /**
     * Set the user query.
     * @param userQuery The user query.
     */
    public void setUserQuery(String userQuery) {
        this.userQuery = userQuery;
    }

}
