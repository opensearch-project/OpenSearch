/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Defines the scope and who this scope is shared with
 *
 * @opensearch.experimental
 */
public class SharedWithScope implements ToXContentFragment {

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
        return builder.startObject().field(scope, sharedWithPerScope).endObject();
    }

    @Override
    public String toString() {
        return "SharedWithScope {" + scope + ": " + sharedWithPerScope + '}';
    }

    public static class SharedWithPerScope implements ToXContentFragment {
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
            return "ShareWith {" + "users=" + users + ", roles=" + roles + ", backendRoles=" + backendRoles + '}';
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.startObject().field("users", users).field("roles", roles).field("backend_roles", backendRoles).endObject();
        }
    }
}
