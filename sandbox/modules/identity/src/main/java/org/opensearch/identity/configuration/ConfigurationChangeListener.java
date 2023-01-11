/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.configuration;

import java.util.Map;

/**
 * Callback function on change particular configuration
 */
public interface ConfigurationChangeListener {

    /**
     * @param typeToConfig not null updated configuration on that was subscribe current listener
     */
    void onChange(Map<CType, SecurityDynamicConfiguration<?>> typeToConfig);
}
