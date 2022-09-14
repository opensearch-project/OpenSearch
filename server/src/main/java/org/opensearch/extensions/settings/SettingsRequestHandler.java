/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.settings;

import org.opensearch.common.settings.SettingsModule;
import org.opensearch.extensions.DiscoveryExtension;
import org.opensearch.extensions.ExtensionStringResponse;
import org.opensearch.transport.TransportResponse;
import org.opensearch.transport.TransportService;

import java.util.Map;

/**
 * Handles requests to register extension settings.
 *
 * @opensearch.internal
 */
public class SettingsRequestHandler {

    private final SettingsModule settingsModule;
    private final Map<String, DiscoveryExtension> extensionIdMap;
    private final TransportService transportService;

    /**
     * Instantiates a new Settings Request Handler using the Node's SettingsModule.
     *
     * @param settingsModule  The Node's {@link SettingsModule}.
     * @param extensionIdMap  A map of extension uniqueId to DiscoveryExtension
     * @param transportService  The Node's transportService
     */
    public SettingsRequestHandler(
        SettingsModule settingsModule,
        Map<String, DiscoveryExtension> extensionIdMap,
        TransportService transportService
    ) {
        this.settingsModule = settingsModule;
        this.extensionIdMap = extensionIdMap;
        this.transportService = transportService;
    }

    /**
     * Handles a {@link RegisterSettingsRequest}.
     *
     * @param settingsRequest  The request to handle.
     * @return A {@link ExtensionStringResponse} indicating success.
     * @throws Exception if the request is not handled properly.
     */
    public TransportResponse handleRegisterSettingsRequest(RegisterSettingsRequest settingsRequest) throws Exception {
        DiscoveryExtension discoveryExtension = extensionIdMap.get(settingsRequest.getUniqueId());
        settingsModule.registerDynamicSetting(null);
        return new ExtensionStringResponse(
            "Registered extension " + settingsRequest.getUniqueId() + " to handle settings " + settingsRequest.getSettings()
        );
    }
}
