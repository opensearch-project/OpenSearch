/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

import org.opensearch.authn.tokens.AuthenticationToken;

import java.security.Principal;
import java.util.List;

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
     * The subject will confirm if it has the permissions that are checked against, if it has those permissions it returns null, otherwise it returns the exception.
     * Note; this method not throw the exception
     * */
    UnauthorizedException checkPermission(List<String> permissions);
}
