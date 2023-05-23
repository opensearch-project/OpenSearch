/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.settings.Setting;

import java.util.Collections;
import java.util.List;

/**
 * Plugin that provides extra settings for extensions
 *
 * @opensearch.experimental
 */
public interface ExtensionAwarePlugin {

    /**
     * Returns a list of additional {@link Setting} definitions that this plugin adds for extensions
     */
    default List<Setting<?>> getExtensionSettings() {
        return Collections.emptyList();
    }
}
