/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro.realm;

import org.opensearch.identity.NamedPrincipal;

/**
 * A non-volatile and immutable object in the storage.
 *
 * @opensearch.experimental
 */
public class User {

    private NamedPrincipal username;
    private String bcryptHash;

    /**
     * Get the User's username as a NamedPrincipal
     * @return the named principal representing the username
     */
    public NamedPrincipal getUsername() {
        return username;
    }

    /**
     * Set the User's username
     * @param username NamedPrincipal representing the username to be set
     */
    public void setUsername(NamedPrincipal username) {
        this.username = username;
    }

    /**
     * Get the User's hash
     * @return The User's hash as a string
     */
    public String getBcryptHash() {
        return bcryptHash;
    }

    /**
     * Set the User's hash
     * @param bcryptHash A string to be set as the User's hash
     */
    public void setBcryptHash(String bcryptHash) {
        this.bcryptHash = bcryptHash;
    }
}
