/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.opensearch.authn.StringPrincipal;
import org.opensearch.identity.authz.OpenSearchPermission;
import org.opensearch.identity.authz.PermissionStorage;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A non-volatile and immutable object in the storage.
 *
 * @opensearch.experimental
 */

public class User {

    // Needed for InternalUsersStore.readUsersAsMap
    public User() {}

    /**
     * Create a new user
     *
     * @param username The username (must not be null or empty)
     * @param bcryptHash The hashed password (must not be null or empty)
     * @param attributes A map of custom attributes
     * @throws IllegalArgumentException if username or bcryptHash is null or empty
     */
    public User(final String username, final String bcryptHash, Map<String, String> attributes, List<OpenSearchPermission> permissions) {
        Objects.requireNonNull(username);
        Objects.requireNonNull(bcryptHash);
        this.username = new StringPrincipal(username);
        this.bcryptHash = bcryptHash;
        this.attributes = attributes;
        this.permissions = permissions;
    }

    @JsonProperty(value = "username")
    private StringPrincipal username;

    @JsonProperty(value = "hash")
    private String bcryptHash;

    @JsonProperty(value = "attributes")
    private Map<String, String> attributes = Collections.emptyMap();

    @JsonProperty(value = "permissions")
    private List<OpenSearchPermission> permissions = this.permissions;

    @JsonProperty(value = "username")
    public StringPrincipal getUsername() {
        return username;
    }

    @JsonProperty(value = "username")
    public void setUsername(StringPrincipal username) {
        this.username = username;
    }

    @JsonProperty(value = "hash")
    public String getBcryptHash() {
        return bcryptHash;
    }

    @JsonProperty(value = "hash")
    public void setBcryptHash(String bcryptHash) {
        this.bcryptHash = bcryptHash;
    }

    @JsonProperty(value = "permissions")
    public List<OpenSearchPermission> getPermissions() {
        return PermissionStorage.get(this.username);
    }

    @JsonProperty(value = "permissions")
    public void setPermissions(List<OpenSearchPermission> permissions) {
        this.permissions = permissions;
    }

    @JsonProperty(value = "attributes")
    public Map<String, String> getAttributes() {
        return attributes;
    }

    @JsonProperty(value = "attributes")
    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "User [username=" + username + ", bcryptHash=" + bcryptHash + ", attributes=" + attributes + "]";
    }
}
