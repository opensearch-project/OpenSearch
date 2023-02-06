/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.extensions;

import org.greenrobot.eventbus.Subscribe;
import org.opensearch.identity.configuration.SecurityDynamicConfiguration;

/**
 * @opensearch.experimental
 */
public class ExtensionSecurityConfigStore {

    private static ExtensionSecurityConfigStore INSTANCE;

    private SecurityDynamicConfiguration<ExtensionSecurityConfig> extensionsSecurityConfig;

    private ExtensionSecurityConfigStore() {}

    public static ExtensionSecurityConfigStore getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new ExtensionSecurityConfigStore();
        }

        return INSTANCE;
    }

    public String getSigningKeyForExtension(String extensionId) {
        String signingKey = null;
        if (this.extensionsSecurityConfig != null && this.extensionsSecurityConfig.getCEntries() != null) {
            if (this.extensionsSecurityConfig.exists(extensionId)) {
                ExtensionSecurityConfig extensionSecurityConfig = this.extensionsSecurityConfig.getCEntry(extensionId);
                signingKey = extensionSecurityConfig.getSigningKey();
            }
        }
        return signingKey;
    }

    @Subscribe
    public void onExtensionsSecurityConfigChanged(SecurityDynamicConfiguration<ExtensionSecurityConfig> esm) {
        this.extensionsSecurityConfig = esm;
    }
}
