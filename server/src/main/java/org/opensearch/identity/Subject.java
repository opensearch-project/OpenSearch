/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.identity.Scope;
import org.opensearch.identity.tokens.AuthToken;

import java.security.Principal;
import java.util.List;
import org.opensearch.common.Nullable;

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
     * Get the application-wide uniquely identifying associated application's principal
     * @return Null, if there is not associated application
     */
    @Nullable
    Principal getAssociatedApplication();

    /**
     * Checks scopes of the current subject if they are allowed for any of the listed scopes
     * @param scope The scopes to check against the subject
     * @return true if allowed, false if none of the scopes are allowed.
     */
    boolean isAllowed(final List<Scope> scope);
}
