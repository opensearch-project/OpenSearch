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
 * This class represents the scope at which a resource is shared with.
 * Example:
 * "read_only": {
 *      "users": [],
 *      "roles": [],
 *      "backend_roles": []
 * }
 *
 * @opensearch.experimental
 */
public class SharedWithScope implements ToXContentFragment, NamedWriteable {

    private final String scope;

    private final ScopeRecipients scopeRecipients;

    public SharedWithScope(String scope, ScopeRecipients scopeRecipients) {
        this.scope = scope;
        this.scopeRecipients = scopeRecipients;
    }

    public SharedWithScope(StreamInput in) throws IOException {
        this.scope = in.readString();
        this.scopeRecipients = new ScopeRecipients(in);
    }

    public String getScope() {
        return scope;
    }

    public ScopeRecipients getSharedWithPerScope() {
        return scopeRecipients;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(scope);
        builder.startObject();

        scopeRecipients.toXContent(builder, params);

        return builder.endObject();
    }

    public static SharedWithScope fromXContent(XContentParser parser) throws IOException {
        String scope = parser.currentName();

        parser.nextToken();

        ScopeRecipients scopeRecipients = ScopeRecipients.fromXContent(parser);

        return new SharedWithScope(scope, scopeRecipients);
    }

    @Override
    public String toString() {
        return "{" + scope + ": " + scopeRecipients + '}';
    }

    @Override
    public String getWriteableName() {
        return "shared_with_scope";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(scope);
        out.writeNamedWriteable(scopeRecipients);
    }

    /**
     * This class represents the entities with whom a resource is shared with for a given scope.
     *
     * @opensearch.experimental
     */
    public static class ScopeRecipients implements ToXContentFragment, NamedWriteable {
        private static final String USERS_FIELD = EntityType.USERS.toString();
        private static final String ROLES_FIELD = EntityType.ROLES.toString();
        private static final String BACKEND_ROLES_FIELD = EntityType.BACKEND_ROLES.toString();

        private Set<String> users;

        private Set<String> roles;

        private Set<String> backendRoles;

        public ScopeRecipients(Set<String> users, Set<String> roles, Set<String> backendRoles) {
            this.users = users;
            this.roles = roles;
            this.backendRoles = backendRoles;
        }

        public ScopeRecipients(StreamInput in) throws IOException {
            this.users = Set.of(in.readStringArray());
            this.roles = Set.of(in.readStringArray());
            this.backendRoles = Set.of(in.readStringArray());
        }

        public Set<String> getUsers() {
            return users;
        }

        public void setUsers(Set<String> users) {
            this.users = users;
        }

        public Set<String> getRoles() {
            return roles;
        }

        public void setRoles(Set<String> roles) {
            this.roles = roles;
        }

        public Set<String> getBackendRoles() {
            return backendRoles;
        }

        public void setBackendRoles(Set<String> backendRoles) {
            this.backendRoles = backendRoles;
        }

        @Override
        public String toString() {
            return "{"
                + USERS_FIELD
                + "="
                + users
                + ", "
                + ROLES_FIELD
                + "="
                + roles
                + ", "
                + BACKEND_ROLES_FIELD
                + "="
                + backendRoles
                + '}';
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            writeFieldOrEmptyArray(builder, USERS_FIELD, users);
            writeFieldOrEmptyArray(builder, ROLES_FIELD, roles);
            writeFieldOrEmptyArray(builder, BACKEND_ROLES_FIELD, backendRoles);
            return builder;
        }

        public static ScopeRecipients fromXContent(XContentParser parser) throws IOException {
            Set<String> users = new HashSet<>();
            Set<String> roles = new HashSet<>();
            Set<String> backendRoles = new HashSet<>();

            XContentParser.Token token;
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (USERS_FIELD.equals(currentFieldName)) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            users.add(parser.text());
                        }
                    } else if (ROLES_FIELD.equals(currentFieldName)) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            roles.add(parser.text());
                        }
                    } else if (BACKEND_ROLES_FIELD.equals(currentFieldName)) {
                        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                            backendRoles.add(parser.text());
                        }
                    } else {
                        parser.skipChildren();
                    }
                }
            }

            return new ScopeRecipients(users, roles, backendRoles);
        }

        private void writeFieldOrEmptyArray(XContentBuilder builder, String fieldName, Set<String> values) throws IOException {
            if (values != null) {
                builder.field(fieldName, values);
            } else {
                builder.array(fieldName);
            }
        }

        @Override
        public String getWriteableName() {
            return "scope_recipients";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(users.toArray(new String[0]));
            out.writeStringArray(roles.toArray(new String[0]));
            out.writeStringArray(backendRoles.toArray(new String[0]));
        }
    }
}
