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
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.example.stream.benchmark.BenchmarkStreamAction;
import org.opensearch.example.stream.benchmark.RestBenchmarkStreamAction;
import org.opensearch.example.stream.benchmark.TransportBenchmarkStreamAction;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestHandler;
import org.opensearch.threadpool.ExecutorBuilder;
import org.opensearch.threadpool.FixedExecutorBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

/**
 * Example plugin demonstrating streaming transport actions
 */
public class StreamTransportExamplePlugin extends Plugin implements ActionPlugin {

    /** Benchmark thread pool name */
    public static final String BENCHMARK_THREAD_POOL_NAME = "benchmark";
    /** Benchmark response handler thread pool name */
    public static final String BENCHMARK_RESPONSE_POOL_NAME = "benchmark_response";

    /**
     * Constructor
     */
    public StreamTransportExamplePlugin() {}

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        int processors = Runtime.getRuntime().availableProcessors();
        int benchmarkSize = settings.getAsInt("thread_pool." + BENCHMARK_THREAD_POOL_NAME + ".size", processors * 8);
        int benchmarkQueue = settings.getAsInt("thread_pool." + BENCHMARK_THREAD_POOL_NAME + ".queue_size", 1000);
        int responseSize = settings.getAsInt("thread_pool." + BENCHMARK_RESPONSE_POOL_NAME + ".size", processors * 8);
        int responseQueue = settings.getAsInt("thread_pool." + BENCHMARK_RESPONSE_POOL_NAME + ".queue_size", 1000);

        return Arrays.asList(
            new FixedExecutorBuilder(
                settings,
                BENCHMARK_THREAD_POOL_NAME,
                benchmarkSize,
                benchmarkQueue,
                "thread_pool." + BENCHMARK_THREAD_POOL_NAME
            ),
            new FixedExecutorBuilder(
                settings,
                BENCHMARK_RESPONSE_POOL_NAME,
                responseSize,
                responseQueue,
                "thread_pool." + BENCHMARK_RESPONSE_POOL_NAME
            )
        );
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionHandler<>(StreamDataAction.INSTANCE, TransportStreamDataAction.class),
            new ActionHandler<>(BenchmarkStreamAction.INSTANCE, TransportBenchmarkStreamAction.class)
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
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        return List.of(new RestBenchmarkStreamAction());
    }
}
