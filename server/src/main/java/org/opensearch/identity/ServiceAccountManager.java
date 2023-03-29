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

public class ServiceAccountManager {

    Settings settings;
    Path extensionsPath;
    public ServiceAccountManager(Settings settings, Path extensionsPath) {
        this.settings = settings;
        this.extensionsPath = extensionsPath;
    }

    public String getOrCreateServiceAccount(String extensionId) {
        String authenticationString = "";
        //TODO: Make this hook into the security plugin
        return authenticationString;
    }


}
