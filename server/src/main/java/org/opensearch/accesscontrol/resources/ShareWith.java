/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * This class contains information about whom a resource is shared with and at what scope.
 * Example:
 * "share_with": {
 *       "read_only": {
 *          "users": [],
 *          "roles": [],
 *          "backend_roles": []
 *       },
 *       "read_write": {
 *          "users": [],
 *          "roles": [],
 *          "backend_roles": []
 *       }
 *    }
 *
 * @opensearch.experimental
 */
public class ShareWith implements ToXContentFragment, NamedWriteable {

    /**
     * A set of objects representing the scopes and their associated users, roles, and backend roles.
     */
    private final Set<SharedWithScope> sharedWithScopes;

    public ShareWith(Set<SharedWithScope> sharedWithScopes) {
        this.sharedWithScopes = sharedWithScopes;
    }

    public ShareWith(StreamInput in) throws IOException {
        this.sharedWithScopes = in.readSet(SharedWithScope::new);
    }

    public Set<SharedWithScope> getSharedWithScopes() {
        return sharedWithScopes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();

        for (SharedWithScope scope : sharedWithScopes) {
            scope.toXContent(builder, params);
        }

        return builder.endObject();
    }

    public static ShareWith fromXContent(XContentParser parser) throws IOException {
        Set<SharedWithScope> sharedWithScopes = new HashSet<>();

        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }

        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            // Each field in the object represents a SharedWithScope
            if (token == XContentParser.Token.FIELD_NAME) {
                SharedWithScope scope = SharedWithScope.fromXContent(parser);
                sharedWithScopes.add(scope);
            }
        }

        return new ShareWith(sharedWithScopes);
    }

    @Override
    public String getWriteableName() {
        return "share_with";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(sharedWithScopes);
    }

    @Override
    public String toString() {
        return "ShareWith " + sharedWithScopes;
    }
}
