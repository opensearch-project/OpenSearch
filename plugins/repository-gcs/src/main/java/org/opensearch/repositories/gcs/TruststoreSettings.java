/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import org.opensearch.core.common.Strings;
import org.opensearch.core.common.settings.SecureString;

import java.nio.file.Path;

/**
 * Encapsulates truststore configuration for Google Cloud Storage client.
 * Provides validation and convenient access to truststore settings.
 */
public record TruststoreSettings(Path path, String type, SecureString password) {

    public static final TruststoreSettings NO_TRUSTSTORE_SETTINGS = new TruststoreSettings(null, null, null);

    public boolean isConfigured() {
        return path != null && Strings.hasText(type);
    }

}
