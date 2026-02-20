/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.eqe.action.QueryAction;
import org.opensearch.eqe.action.ShardQueryAction;
import org.opensearch.eqe.action.TransportQueryAction;
import org.opensearch.eqe.action.TransportShardQueryAction;
import org.opensearch.eqe.engine.ShardQueryExecutor;
import org.opensearch.eqe.engine.EngineContext;
import org.opensearch.eqe.engine.ExecutionEngine;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * Calcite-based query engine plugin.
 *
 * <p>Provides query execution infrastructure that accepts Calcite RelNode plans
 * and executes them across OpenSearch data nodes, returning Arrow columnar results.
 *
 * <p>The engine implementation (e.g., engine-datafusion) is discovered via
 * {@link ExtensiblePlugin} SPI. The extending plugin provides a
 * {@link ExecutionEngine} implementation which is loaded in
 * {@link #loadExtensions} and made available via {@link #getExecutor()}.
 */
public class ExtensibleQueryEnginePlugin extends Plugin implements ActionPlugin, ExtensiblePlugin {

    private static final Logger logger = LogManager.getLogger(ExtensibleQueryEnginePlugin.class);

    private BufferAllocator allocator;
    protected ExecutionEngine executor;

    @Override
    public void loadExtensions(ExtensionLoader loader) {
        List<ExecutionEngine> executors = loader.loadExtensions(ExecutionEngine.class);
        if (executors.isEmpty()) {
            logger.info("No NativeEngineExecutor extensions found");
        } else {
            if (executors.size() > 1) {
                logger.warn("Multiple NativeEngineExecutor extensions found, using first: {}", executors.get(0).getClass().getName());
            }
            this.executor = executors.get(0);
            logger.info("Loaded NativeEngineExecutor: {}", executor.getClass().getName());
        }
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
            Supplier<RepositoriesService> repositoriesServiceSupplier) {

        this.allocator = new RootAllocator();
        EngineContext engineContext = new EngineContext(executor, allocator);
        ShardQueryExecutor shardQueryExecutor = new ShardQueryExecutor(executor, allocator);
        return List.of(engineContext, shardQueryExecutor);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(ShardQueryAction.INSTANCE, TransportShardQueryAction.class),
            new ActionHandler<>(QueryAction.INSTANCE, TransportQueryAction.class)
        );
    }

    public ExecutionEngine getExecutor() {
        return executor;
    }

    public BufferAllocator getAllocator() {
        return allocator;
    }

    @Override
    public void close() {
        if (allocator != null) {
            allocator.close();
        }
    }
}
