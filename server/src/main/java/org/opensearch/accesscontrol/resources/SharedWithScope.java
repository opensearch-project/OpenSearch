/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Defines the scope and who this scope is shared with
 *
 * @opensearch.experimental
 */
public class SharedWithScope implements ToXContentFragment, NamedWriteable {

    private final String scope;

    private final SharedWithPerScope sharedWithPerScope;

    public SharedWithScope(String scope, SharedWithPerScope sharedWithPerScope) {
        this.scope = scope;
        this.sharedWithPerScope = sharedWithPerScope;
    }

    public String getScope() {
        return scope;
    }

    public SharedWithPerScope getSharedWithPerScope() {
        return sharedWithPerScope;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(scope);
        builder.startObject();

        sharedWithPerScope.toXContent(builder, params);

        return builder.endObject();
    }

    @Override
    public String toString() {
        return "{" + scope + ": " + sharedWithPerScope + '}';
    }

    @Override
    public String getWriteableName() {
        return "shared_with_scope";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(scope);
        out.writeNamedWriteable(sharedWithPerScope);
    }

    /**
     * This class defines who a resource is shared_with for a particular scope
     *
     * @opensearch.experimental
     */
    public static class SharedWithPerScope implements ToXContentFragment, NamedWriteable {
        private List<String> users;

        private List<String> roles;

        private List<String> backendRoles;

        public SharedWithPerScope(List<String> users, List<String> roles, List<String> backendRoles) {
            this.users = users;
            this.roles = roles;
            this.backendRoles = backendRoles;
        }

        public List<String> getUsers() {
            return users;
        }

        public void setUsers(List<String> users) {
            this.users = users;
        }

        public List<String> getRoles() {
            return roles;
        }

        public void setRoles(List<String> roles) {
            this.roles = roles;
        }

        public List<String> getBackendRoles() {
            return backendRoles;
        }

        public void setBackendRoles(List<String> backendRoles) {
            this.backendRoles = backendRoles;
        }

        @Override
        public String toString() {
            return "{" + "users=" + users + ", roles=" + roles + ", backendRoles=" + backendRoles + '}';
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            writeFieldOrEmptyArray(builder, "users", users);
            writeFieldOrEmptyArray(builder, "roles", roles);
            writeFieldOrEmptyArray(builder, "backend_roles", backendRoles);
            return builder;
        }

        private void writeFieldOrEmptyArray(XContentBuilder builder, String fieldName, List<String> values) throws IOException {
            if (values != null) {
                builder.field(fieldName, values);
            } else {
                builder.array(fieldName);
            }
        }

        @Override
        public String getWriteableName() {
            return "shared_with_per_scope";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeStringArray(users.toArray(new String[0]));
            out.writeStringArray(roles.toArray(new String[0]));
            out.writeStringArray(backendRoles.toArray(new String[0]));
        }
    }
}
