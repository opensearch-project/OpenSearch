/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.plugin;

import org.opensearch.arrow.memory.ArrowAllocatorService;
import org.opensearch.arrow.memory.DefaultArrowAllocatorService;
import org.opensearch.arrow.transport.ArrowBatchResponse;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.AbstractModule;
import org.opensearch.common.inject.Module;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Node-level home for Apache Arrow. Bundles the Arrow libraries and exposes the transport
 * integration contracts ({@link ArrowBatchResponse} and friends) plus the node-level
 * {@link ArrowAllocatorService}. Plugins that produce or consume native Arrow data declare
 * {@code extendedPlugins = ['arrow-base']} so they share this plugin's classloader — a
 * single copy of the Arrow classes lets Arrow-carrying transport types cross plugin
 * boundaries.
 *
 * <p>Implements {@link ExtensiblePlugin} purely for the classloader side-effect of being
 * a valid parent in {@code extendedPlugins} chains. This plugin does not define or consume
 * any SPI — do not add {@code loadExtensions()} hooks here.
 */
public class ArrowBasePlugin extends Plugin implements ExtensiblePlugin {

    private DefaultArrowAllocatorService allocatorService;

    /** Default constructor. */
    public ArrowBasePlugin() {}

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        this.allocatorService = new DefaultArrowAllocatorService();
        return List.of(allocatorService);
    }

    @Override
    public Collection<Module> createGuiceModules() {
        return List.of(new AbstractModule() {
            @Override
            protected void configure() {
                bind(ArrowAllocatorService.class).toInstance(allocatorService);
            }
        });
    }

    @Override
    public void close() throws IOException {
        if (allocatorService != null) {
            allocatorService.close();
        }
    }
}
