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

public class User implements Subject {

    String username;
    String password;

    public User(String username) {
        username = username;
    }

    @Override
    public Principal getPrincipal() {
        return new NamedPrincipal(this.username);
    }

    @Override
    public void authenticate(AuthToken token) {

    }
}
