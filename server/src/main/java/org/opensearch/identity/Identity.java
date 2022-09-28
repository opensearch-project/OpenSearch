/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

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
