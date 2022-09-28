/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.noop;

import org.opensearch.identity.AccessTokenManager;
import org.opensearch.identity.AuthenticationManager;
import org.opensearch.identity.Subject;

/**
 * Implementation of authentication manager that does not enforce authentication
 *
 * This class and related classes in this package will not return nulls or fail permissions checks
 *
 * @opensearch.internal
 */
public class NoopAuthenticationManager implements AuthenticationManager {

    @Override
    public Subject getSubject() {
        return new NoopSubject();
    }

    @Override
    public AccessTokenManager getAccessTokenManager() {
        return null;
    }
}
