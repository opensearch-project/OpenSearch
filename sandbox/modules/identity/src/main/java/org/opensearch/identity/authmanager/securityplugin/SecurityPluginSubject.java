/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.authmanager.securityplugin;

import org.opensearch.authn.StringPrincipal;
import org.opensearch.authn.Subject;
import org.opensearch.authn.tokens.AuthenticationToken;

import java.security.Principal;

public class SecurityPluginSubject implements Subject {
    private StringPrincipal name;

    /**
     * Create a new authenticated user without attributes
     *
     * @param name The username (must not be null or empty)
     */
    public SecurityPluginSubject(final String name) {
        this.name = new StringPrincipal(name);
    }

    @Override
    public Principal getPrincipal() {
        return name;
    }

    @Override
    public void login(AuthenticationToken token) {
        // Security Plugin authentication is performed on the REST-layer using getRestHandlerWrapper extension point
        return;
    }

    @Override
    public boolean isPermitted(String permission) {
        return false;
    }
}
