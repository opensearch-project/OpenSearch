/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions;

import java.io.IOException;
import java.util.Optional;
import java.util.Set;

import org.opensearch.action.ActionModule;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsModule;

import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.extensions.action.ExtensionActionResponse;
import org.opensearch.extensions.action.RemoteExtensionActionResponse;
import org.opensearch.transport.TransportService;

/**
 * Noop class for ExtensionsManager
 *
 * @opensearch.internal
 */
public class NoopExtensionsManager extends ExtensionsManager {

    public NoopExtensionsManager() throws IOException {
        super(Set.of());
    }

    @Override
    public void initializeServicesAndRestHandler(
        ActionModule actionModule,
        SettingsModule settingsModule,
        TransportService transportService,
        ClusterService clusterService,
        Settings initialEnvironmentSettings,
        NodeClient client
    ) {
        // no-op
    }

    @Override
    public RemoteExtensionActionResponse handleRemoteTransportRequest(ExtensionActionRequest request) throws Exception {
        // no-op empty response
        return new RemoteExtensionActionResponse(true, new byte[0]);
    }

    @Override
    public ExtensionActionResponse handleTransportRequest(ExtensionActionRequest request) throws Exception {
        // no-op empty response
        return new ExtensionActionResponse(new byte[0]);
    }

    @Override
    public void initialize() {
        // no-op
    }

    @Override
    public Optional<DiscoveryExtensionNode> lookupInitializedExtensionById(final String extensionId) {
        // no-op not found
        return Optional.empty();
    }
}
