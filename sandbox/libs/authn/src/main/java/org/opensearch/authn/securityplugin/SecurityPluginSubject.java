/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn.securityplugin;

import org.opensearch.authn.Subject;
import org.opensearch.authn.tokens.AuthenticationToken;

import java.security.Principal;

public class SecurityPluginSubject implements Subject {
    @Override
    public Principal getPrincipal() {
        return null;
    }

    @Override
    public void login(AuthenticationToken token) {
        // Security Plugin authentication is performed on the REST-layer using getRestHandlerWrapper extension point
        return;
    }
}
