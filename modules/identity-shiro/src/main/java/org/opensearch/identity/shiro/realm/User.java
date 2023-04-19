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

    public NamedPrincipal getUsername() {
        return username;
    }

    public void setUsername(NamedPrincipal username) {
        this.username = username;
    }

    public String getBcryptHash() {
        return bcryptHash;
    }

    public void setBcryptHash(String bcryptHash) {
        this.bcryptHash = bcryptHash;
    }
}
