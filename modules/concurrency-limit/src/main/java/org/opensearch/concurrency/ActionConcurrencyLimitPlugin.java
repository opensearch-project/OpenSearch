/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.concurrency;

import org.opensearch.action.ActionConcurrencyLimiterStats;
import org.opensearch.action.support.ActionFilter;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ConcurrencyLimiterStatsPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.TelemetryAwarePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.rest.RestHeaderDefinition;
import org.opensearch.script.ScriptService;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Module plugin that enables adaptive per-action concurrency limiting.
 * <p>
 * Operators configure limiters via cluster settings using an alias as the namespace key:
 * <pre>
 *   PUT /_cluster/settings
 *   { "persistent": {
 *       "concurrency_limit.action.search.action_name": "indices:data/read/search",
 *       "concurrency_limit.action.search.mode": "enforced",
 *       "concurrency_limit.action.search.algorithm": "vegas"
 *   }}
 * </pre>
 * Any OpenSearch action can be limited — no code change is required.
 */
public class ActionConcurrencyLimitPlugin extends Plugin implements ActionPlugin, ConcurrencyLimiterStatsPlugin, TelemetryAwarePlugin {

    /** Creates a new {@link ActionConcurrencyLimitPlugin}. */
    public ActionConcurrencyLimitPlugin() {}

    /**
     * HTTP request header that the {@code byHeader} partition resolver reads to determine a
     * request's partition. Registered via {@link #getRestHeaders()} so it is copied from the
     * REST request into the transport {@link org.opensearch.common.util.concurrent.ThreadContext},
     * where the action filter can read it.
     * <p>
     * Note: REST headers must be registered at node startup. Only this fixed header name is
     * propagated; configuring {@code partition.resolver.byHeader.name} to a different value will
     * not work unless that header is also registered here.
     */
    public static final String TIER_HEADER = "X-Request-Tier";

    private volatile ActionConcurrencyLimiterRegistry registry;

    @Override
    public Collection<RestHeaderDefinition> getRestHeaders() {
        // Propagate the tier header from REST into the transport ThreadContext so the
        // byHeader partition resolver can read it during the action filter.
        return List.of(new RestHeaderDefinition(TIER_HEADER, false));
    }

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
        registry = new ActionConcurrencyLimiterRegistry(
            clusterService.getSettings(),
            clusterService.getClusterSettings(),
            threadPool.getThreadContext()
        );
        return List.of(registry);
    }

    /**
     * Telemetry-aware component creation. Invoked after the standard {@link #createComponents}
     * (see {@code Node.java}, which runs {@code Plugin.createComponents} before
     * {@code TelemetryAwarePlugin.createComponents}), so {@link #registry} is already initialized.
     * Wires the push-telemetry bridge that mirrors the pull-path ({@code NodeStats}) metrics via the
     * {@link MetricsRegistry}. When telemetry is disabled the node supplies a no-op registry, so the
     * publisher is inert.
     *
     * @param client the node client
     * @param clusterService the cluster service
     * @param threadPool the thread pool
     * @param resourceWatcherService the resource watcher service
     * @param scriptService the script service
     * @param xContentRegistry the XContent registry
     * @param environment the node environment settings
     * @param nodeEnvironment the node environment
     * @param namedWriteableRegistry the named writeable registry
     * @param indexNameExpressionResolver the index name expression resolver
     * @param repositoriesServiceSupplier the repositories service supplier
     * @param tracer the tracer
     * @param metricsRegistry the metrics registry
     */
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
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        MetricsRegistry metricsRegistry
    ) {
        ConcurrencyLimiterMetricsPublisher publisher = new ConcurrencyLimiterMetricsPublisher(metricsRegistry);
        registry.setMetricsListener(publisher);
        return List.of(publisher);
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return List.of(new ActionConcurrencyLimitFilter(registry));
    }

    @Override
    public List<Setting<?>> getSettings() {
        return ActionConcurrencyLimiterRegistry.ALL_SETTINGS;
    }

    @Override
    public ActionConcurrencyLimiterStats getConcurrencyLimiterStats() {
        ActionConcurrencyLimiterRegistry r = this.registry;
        return r != null ? r.getStats() : null;
    }
}
