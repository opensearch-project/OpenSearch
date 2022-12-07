/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

import org.opensearch.authn.tokens.AuthenticationToken;

import java.security.Principal;

/**
 * An individual, process, or device that causes information to flow among objects or change to the system state.
 *
 * @opensearch.experimental
 */
public interface Subject {

    /**
     * Get the application-wide uniquely identifying principal
     * */
    Principal getPrincipal();

    /**
     * Authentication check via the token
     * throws UnsupportedAuthenticationMethod
     * throws InvalidAuthenticationToken
     * throws SubjectNotFound
     * throws SubjectDisabled
     */
    void login(final AuthenticationToken token);

    /**
     * Logs this subject out and kills any session associated with it
     */
    void logout();

    /**
     * Checks the current authentication status of this subject
     * @return true if authenticated, false otherwise
     */
    boolean isAuthenticated();
}
