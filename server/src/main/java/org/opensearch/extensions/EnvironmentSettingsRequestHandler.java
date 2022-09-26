/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import org.opensearch.common.settings.Settings;
import org.opensearch.env.EnvironmentSettingsResponse;
import org.opensearch.transport.TransportResponse;

/**
 * Handles requests to retrieve environment settings.
 *
 * @opensearch.internal
 */
public class EnvironmentSettingsRequestHandler {

    private final Settings initialEnvironmentSettings;

    /**
     * Instantiates a new Environment Settings Request Handler with the environment settings
     *
     * @param initialEnvironmentSettings the finalized view of environment {@link Settings}
     */
    public EnvironmentSettingsRequestHandler(Settings initialEnvironmentSettings) {
        this.initialEnvironmentSettings = initialEnvironmentSettings;
    }

    /**
     * Handles a {@link EnvironmentSettingsRequest}.
     *
     * @param environmentSettingsRequest  The request to handle.
     * @return  A {@link EnvironmentSettingsResponse}
     * @throws Exception if the request is not handled properly.
     */
    TransportResponse handleEnvironmentSettingsRequest(EnvironmentSettingsRequest environmentSettingsRequest) throws Exception {
        return new EnvironmentSettingsResponse(this.initialEnvironmentSettings, environmentSettingsRequest.getComponentSettings());
    }
}
