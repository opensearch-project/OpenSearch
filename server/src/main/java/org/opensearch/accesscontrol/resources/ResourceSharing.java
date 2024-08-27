/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.accesscontrol.resources;

import java.util.List;

/**
 * This class contains information about whom a resource is shared with.
 * It could be a user-name, a role or a backend_role.
 *
 * @opensearch.experimental
 */
public class ResourceSharing {

    private List<String> users;

    private List<String> roles;

    private List<String> backendRoles;

    public ResourceSharing(List<String> users, List<String> backendRoles, List<String> roles) {
        this.users = users;
        this.backendRoles = backendRoles;
        this.roles = roles;
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
        return "ResourceSharing {" + "users=" + users + ", roles=" + roles + ", backendRoles=" + backendRoles + '}';
    }
}
