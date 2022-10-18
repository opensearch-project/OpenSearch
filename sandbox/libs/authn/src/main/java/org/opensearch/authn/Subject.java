/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn;

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
     * Logs the user in
     */
    void login(AuthenticationToken authenticationToken);
}
