/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.identity.tokens.AuthToken;

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
     * Authenticate via an auth token
     * throws UnsupportedAuthenticationMethod
     * throws InvalidAuthenticationToken
     * throws SubjectNotFound
     * throws SubjectDisabled
     */
    void authenticate(final AuthToken token);

    /**
     * Checks scopes of the current subject if they are allowed for any of the listed scopes
     * @param scope The scopes to check against the subject
     * @return true if allowed, false if none of the scopes are allowed.
     */
    /// Draft Pull Request Remarks
    // Permissions haven't been implemented yet, and there are good reasons to have permissions and scopes overlap,
    // as well as have disconnected. For the moment lets look past that debate and get feedback around how
    // scope might be added inside of OpenSearch and connected into various systems create security barriers between
    // systems.
    // This will need to be addressed before this change can come out of draft
    boolean isAllowed(final List<Scope> scope);
}
