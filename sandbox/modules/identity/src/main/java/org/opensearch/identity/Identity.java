/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.identity;

import org.opensearch.authn.AuthenticationManager;

/**
 * Application wide access for identity systems
 *
 * @opensearch.experimental
 */
public final class Identity {
    private static AuthenticationManager AUTH_MANAGER = null;

    /**
     * Do not allow instances of this class to be created
     */
    private Identity() {}

    /**
     * Gets the Authentication Manager for this application
     */
    public static AuthenticationManager getAuthManager() {
        return AUTH_MANAGER;
    }

    /**
     * Gets the Authentication Manager for this application
     */
    public static void setAuthManager(final AuthenticationManager authManager) {
        AUTH_MANAGER = authManager;
    }

}
