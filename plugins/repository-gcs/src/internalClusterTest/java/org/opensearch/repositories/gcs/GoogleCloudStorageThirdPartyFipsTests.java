/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.gcs;

import org.opensearch.common.settings.MockSecureSettings;
import org.opensearch.common.settings.SecureSettings;
import org.opensearch.common.settings.Settings;

import java.nio.file.Path;

public class GoogleCloudStorageThirdPartyFipsTests extends GoogleCloudStorageThirdPartyTests {

    private final Path truststorePath = getDataPath("/google.bcfks");

    @Override
    protected Settings nodeSettings() {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings());
        builder.put("gcs.client.default.truststore.path", truststorePath.toString());
        builder.put("gcs.client.default.truststore.type", "BCFKS");
        return builder.build();
    }

    @Override
    protected SecureSettings credentials() {
        MockSecureSettings secureSettings = (MockSecureSettings) super.credentials();
        secureSettings.setString("gcs.client.default.truststore.password", "notasecret");
        return secureSettings;
    }
}
