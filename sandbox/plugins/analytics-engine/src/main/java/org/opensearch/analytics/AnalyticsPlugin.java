/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.backend.EngineCapabilities;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.analytics.schema.SchemaProvider;
import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;
import org.opensearch.cluster.ClusterState;
import org.opensearch.common.inject.Module;
import org.opensearch.common.inject.TypeLiteral;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Analytics engine hub. Implements {@link ExtensiblePlugin} to discover
 * and wire query back-end extensions via SPI.
 *
 * @opensearch.internal
 */
public class AnalyticsPlugin extends Plugin implements ExtensiblePlugin {

    private static final Logger logger = LogManager.getLogger(AnalyticsPlugin.class);

    private final List<AnalyticsBackEndPlugin> backEnds = new ArrayList<>();

    // Lazy references populated in createComponents, resolved by Guice providers
    private final AtomicReference<SchemaProvider> schemaProviderRef = new AtomicReference<>();

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        backEnds.addAll(loader.loadExtensions(AnalyticsBackEndPlugin.class));
        SchemaProvider schemaProvider = clusterState -> OpenSearchSchemaBuilder.buildSchema((ClusterState) clusterState);
        schemaProviderRef.set(schemaProvider);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Module> createGuiceModules() {
        return List.of(b -> {
            b.bind(new TypeLiteral<QueryPlanExecutor<RelNode, Iterable<Object[]>>>() {
            }).toProvider(() -> new DefaultPlanExecutor(backEnds));
            b.bind(SchemaProvider.class).toProvider(schemaProviderRef::get);
            b.bind(EngineCapabilities.class).toInstance(EngineCapabilities.defaultCapabilities());
        });
    }
}
