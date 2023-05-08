/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.identity.tokens.AuthToken;

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
     * Authenticate via an auth token
     * throws UnsupportedAuthenticationMethod
     * throws InvalidAuthenticationToken
     * throws SubjectNotFound
     * throws SubjectDisabled
     */
    void authenticate(final AuthToken token);

    /**
     * Performs an authorization check for the subject if it has permission
     * 
     * @return true if the subject has permission, false if the subject should
     * be denied
     */
    boolean hasPermission(final String permission);

    /**
     * Throws an exception if an authorization check for the subject is denied
     * 
     * @throws UnauthorizedException If the permission is not allowed 
     */
    void checkPermission(final String permission);
}
