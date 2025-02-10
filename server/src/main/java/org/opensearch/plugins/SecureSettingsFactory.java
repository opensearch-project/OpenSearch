/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;

import java.util.Optional;

/**
 * A factory for creating the instance of the {@link SecureTransportSettingsProvider}, taking into account current settings.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SecureSettingsFactory {
    /**
     * Creates (or provides pre-created) instance of the {@link SecureTransportSettingsProvider}
     * @param settings settings
     * @return optionally, the instance of the {@link SecureTransportSettingsProvider}
     */
    Optional<SecureTransportSettingsProvider> getSecureTransportSettingsProvider(Settings settings);

    /**
     * Creates (or provides pre-created) instance of the {@link SecureHttpTransportSettingsProvider}
     * @param settings settings
     * @return optionally, the instance of the {@link SecureHttpTransportSettingsProvider}
     */
    Optional<SecureHttpTransportSettingsProvider> getSecureHttpTransportSettingsProvider(Settings settings);
}
