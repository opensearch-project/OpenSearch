/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.settings.Setting;
import org.opensearch.identity.Subject;

import java.util.Collections;
import java.util.List;

/**
 * Plugin that provides identity and access control for OpenSearch
 *
 * @opensearch.experimental
 */
public interface IdentityPlugin {

    /**
     * Get the current subject
     *
     * Should never return null
     * */
    Subject getSubject();

    /**
     * Returns a list of additional {@link Setting} definitions for that this plugin adds for extensions
     */
    default List<Setting<?>> getExtensionSettings() {
        return Collections.emptyList();
    }
}
