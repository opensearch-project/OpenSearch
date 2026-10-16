/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.IngestionConsumerPlugin;
import org.opensearch.plugins.Plugin;

/**
 * Ingestion plugin handles arrow format in general, the {@link IngestionSource#getType()} should be
 * {@link #TYPE}, and configs should be specified in {@link IngestionSource#params()}, see {@link
 * ArrowIngestionConfig} for details.
 *
 * <p>This plugin is deliberately dataset-agnostic: it knows how to decode Arrow {@code
 * VectorSchemaRoot} batches into OpenSearch index operations, but it has no built-in notion of
 * where those batches come from. Concrete data sources (Lance, Iceberg, Delta Lake, or an
 * organization's own proprietary format) are contributed by other plugins that implement {@link
 * ArrowSourceFactory} and declare {@code extendedPlugins = ['ingestion-arrow']}; see {@link
 * #loadExtensions} and {@link ArrowSourceFactory} for the extension contract.
 */
public class ArrowPlugin extends Plugin implements IngestionConsumerPlugin, ExtensiblePlugin {

    private static final Logger LOGGER = LogManager.getLogger(ArrowPlugin.class);

    /** The ingestion source type this plugin handles, see {@link IngestionSource#getType()} */
    public static final String TYPE = "ARROW";

    // Populated by loadExtensions before getIngestionConsumerFactories is ever called: the node
    // bootstrap sequence runs ExtensiblePlugin#loadExtensions on every installed plugin before
    // it collects ingestion consumer factories (see PluginsService#loadExtensions / Node's
    // ingestionConsumerFactories wiring).
    private ArrowConsumerFactory arrowConsumerFactory;

    /** No-arg constructor required by the plugin loading framework. */
    public ArrowPlugin() {}

    /**
     * Discovers every {@link ArrowSourceFactory} contributed by plugins that declare this plugin
     * in their {@code extendedPlugins}, via the standard Java {@code ServiceLoader} mechanism, and
     * combines them with the built-in {@link ArrowIpcSourceFactory}. Fails node startup if two
     * factories register the same {@link ArrowSourceFactory#getType()}.
     *
     * @param loader used to discover {@link ArrowSourceFactory} implementations contributed by
     *     extending plugins
     */
    @Override
    public void loadExtensions(ExtensionLoader loader) {
        Map<String, ArrowSourceFactory> sourceFactories = new HashMap<>();
        // Registered directly rather than via the extendedPlugins/ServiceLoader mechanism: a
        // plugin cannot discover its own bundled providers that way.
        registerFactory(sourceFactories, new ArrowIpcSourceFactory());
        for (ArrowSourceFactory factory : loader.loadExtensions(ArrowSourceFactory.class)) {
            registerFactory(sourceFactories, factory);
        }
        this.arrowConsumerFactory = new ArrowConsumerFactory(Collections.unmodifiableMap(sourceFactories));
    }

    private void registerFactory(Map<String, ArrowSourceFactory> sourceFactories, ArrowSourceFactory factory) {
        String type = factory.getType();
        ArrowSourceFactory existing = sourceFactories.putIfAbsent(type, factory);
        if (existing != null) {
            throw new IllegalStateException(
                    "Multiple ArrowSourceFactory implementations registered for type '"
                            + type
                            + "': "
                            + existing.getClass().getName()
                            + " and "
                            + factory.getClass().getName());
        }
        LOGGER.info("Registered ArrowSourceFactory for type '{}': {}", type, factory.getClass().getName());
    }

    @Override
    @SuppressWarnings("rawtypes")
    public Map<String, IngestionConsumerFactory> getIngestionConsumerFactories() {
        return Map.of(TYPE, arrowConsumerFactory);
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
