/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.authn.securityplugin;

import org.opensearch.authn.AccessTokenManager;
import org.opensearch.authn.AuthenticationManager;
import org.opensearch.authn.Subject;

public class SecurityPluginAuthenticationManager implements AuthenticationManager {
    @Override
    public Subject getSubject() {
        return null;
    }

    @Override
    public AccessTokenManager getAccessTokenManager() {
        return null;
    }
}
