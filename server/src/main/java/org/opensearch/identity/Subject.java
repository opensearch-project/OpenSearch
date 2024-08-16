/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.identity.tokens.AuthToken;

import java.security.Principal;
import java.util.concurrent.Callable;

/**
 * An individual, process, or device that causes information to flow among objects or change to the system state.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
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
     * runAs allows the caller to run a callable function as this subject
     */
    default void runAs(Callable<Void> callable) throws Exception {
        callable.call();
    };
}
