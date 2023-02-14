/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.configuration.model;

import org.opensearch.identity.configuration.SecurityDynamicConfiguration;
import org.opensearch.identity.extensions.ExtensionSecurityConfig;

public class ExtensionsSecurityConfigModel {

    private final SecurityDynamicConfiguration<ExtensionSecurityConfig> extensionsSecurityConfigDC;

    public ExtensionsSecurityConfigModel(SecurityDynamicConfiguration<ExtensionSecurityConfig> extensionsSecurityConfigDC) {
        super();
        this.extensionsSecurityConfigDC = extensionsSecurityConfigDC;
    }

    public ExtensionSecurityConfig getExtensionSecurityConfig(String extensionId) {
        return extensionsSecurityConfigDC.getCEntry(extensionId);
    }

    public boolean exists(String extensionId) {
        return extensionsSecurityConfigDC.exists(extensionId);
    }
}
