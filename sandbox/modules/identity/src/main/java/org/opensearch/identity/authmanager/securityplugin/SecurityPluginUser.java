/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.authmanager.securityplugin;

import java.io.Serializable;

public class SecurityPluginUser implements Serializable {
    private String name;

    /**
     * Create a new authenticated user without attributes
     *
     * @param name The username (must not be null or empty)
     */
    public SecurityPluginUser(final String name) {
        this.name = name;
    }

    public final String getName() {
        return name;
    }

    @Override
    public final String toString() {
        return "User [name=" + name + "]";
    }
}
