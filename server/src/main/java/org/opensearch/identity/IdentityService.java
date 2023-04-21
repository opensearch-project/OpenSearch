/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.common.settings.Settings;

import java.nio.file.Path;

/**
 * IdentityService manages ServiceAccountManager
 */
public class IdentityService {

    Settings settings;
    Path extensionPath;

    public IdentityService() {
        this.settings = null;
        this.extensionPath = Path.of("");
    }

    public IdentityService(Settings settings, Path extensionPath) {
        this.settings = settings;
        this.extensionPath = extensionPath;
    }

    public ServiceAccountManager getServiceAccountManager() {
        ServiceAccountManager serviceAccountManager = new ServiceAccountManager(settings, extensionPath);
        return serviceAccountManager;
    }

}
