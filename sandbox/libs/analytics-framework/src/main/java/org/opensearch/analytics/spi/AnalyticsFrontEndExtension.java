/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * SPI for frontend plugins (e.g., opensearch-sql) that integrate with analytics-engine.
 *
 * <p>Implementers are discovered by {@code AnalyticsPlugin} via
 * {@link org.opensearch.plugins.ExtensiblePlugin#loadExtensions}; analytics-engine pushes its
 * services to each consumer once Guice has constructed them. Mirrors the
 * {@code JobSchedulerExtension} pattern from opensearch-job-scheduler — the consumer plugin
 * declares its capability via this interface; the publishing plugin (analytics-engine) handles
 * discovery and lifecycle.
 *
 * <p>This SPI lets a frontend declare analytics-engine as an OPTIONAL extended plugin
 * ({@code extendedPlugins = ['analytics-engine;optional=true']}). When analytics-engine is not
 * installed, no consumer ever receives a callback; analytics-routing code paths stay inert and
 * the frontend plugin boots normally.
 *
 * <p><b>Lifecycle.</b> {@link #setAnalyticsServices} is invoked exactly once per consumer per node
 * lifecycle, AFTER the node Guice injector is built (i.e., after every plugin's
 * {@code createComponents} returns) and BEFORE the first analytics query is dispatched.
 * Implementations should stash the bundle for later use; do not assume the services are available
 * during {@code createComponents}.
 *
 * @opensearch.internal
 */
public interface AnalyticsFrontEndExtension {

    /**
     * Receives the bundle of analytics-engine services. Called exactly once after the services are
     * constructed and before any analytics query is dispatched. Each service inside the bundle is
     * safe to invoke from any thread once received.
     */
    void setAnalyticsServices(AnalyticsServices services);
}
