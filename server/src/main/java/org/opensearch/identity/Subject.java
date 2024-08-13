/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.common.annotation.PublicApi;
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
     * runAs allows the caller to create a session as this subject
     *
     * @return A session to run transport actions in the context of this subject
     */
    Session runAs();

    /**
     * This construct represents a session for this subject. A session is a short-lived block
     * where transport actions are executed as this subject
     *
     * @opensearch.api
     */
    @FunctionalInterface
    @PublicApi(since = "2.17.0")
    interface Session extends AutoCloseable {
        @Override
        void close();
    }
}
