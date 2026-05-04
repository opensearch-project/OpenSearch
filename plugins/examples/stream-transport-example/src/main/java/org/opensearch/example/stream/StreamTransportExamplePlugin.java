/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Module;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.transport.StreamTransportService;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class StreamTransportExamplePlugin extends Plugin implements ActionPlugin {

    private final AtomicReference<StreamTransportService> stsRef = new AtomicReference<>();

    public StreamTransportExamplePlugin() {}

    @Override
    public Collection<Module> createGuiceModules() {
        return List.of(new AbstractModule() {
            @Override
            protected void configure() {
                bind(StreamTransportServiceHolder.class).toInstance(new StreamTransportServiceHolder(stsRef));
            }
        });
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(StreamDataAction.INSTANCE, TransportStreamDataAction.class),
            new ActionHandler<>(NativeArrowStreamDataAction.INSTANCE, TransportNativeArrowStreamDataAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesLookup
    ) {
        return List.of(new RestNativeArrowStreamAction(nodesLookup, stsRef::get));
    }
}
