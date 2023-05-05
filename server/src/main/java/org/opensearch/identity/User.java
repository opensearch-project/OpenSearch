/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import java.security.Principal;
import org.opensearch.identity.tokens.AuthToken;

/**
 * A User object
 */
public class User implements Subject {

    String username;
    String password;

    /**
     * Constructs a User with the given username
     * @param username A String for the User's name
     */
    public User(String username) {
        username = username;
    }

    /**
     * Returns a new NamedPrincipal based on the User's name
     * @return A new principal
     */
    @Override
    public Principal getPrincipal() {
        return new NamedPrincipal(this.username);
    }

    /**
     * Authenticates the User object based on the provided auth token
     * @param token An auth token used for authenticating the user
     */
    @Override
    public void authenticate(AuthToken token) {

    }
}
