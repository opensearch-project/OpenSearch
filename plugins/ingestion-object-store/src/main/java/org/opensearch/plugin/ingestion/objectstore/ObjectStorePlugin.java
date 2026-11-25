/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SetOnce;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.plugins.IngestionConsumerPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A plugin that registers the {@code object_store} pull-based ingestion source. It reads objects at rest in
 * object storage through OpenSearch's {@link org.opensearch.common.blobstore.BlobContainer} abstraction, backed by a
 * registered repository resolved via the {@link RepositoriesService}.
 */
public class ObjectStorePlugin extends Plugin implements IngestionConsumerPlugin {

    /**
     * The registered ingestion source type.
     */
    public static final String TYPE = "OBJECT_STORE";

    private final SetOnce<Supplier<RepositoriesService>> repositoriesServiceSupplier = new SetOnce<>();

    /**
     * Creates the object store ingestion plugin.
     */
    public ObjectStorePlugin() {}

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
        this.repositoriesServiceSupplier.set(repositoriesServiceSupplier);
        return Collections.emptyList();
    }

    @SuppressWarnings("rawtypes")
    @Override
    public Map<String, IngestionConsumerFactory> getIngestionConsumerFactories() {
        return Map.of(TYPE, new ObjectStoreConsumerFactory(() -> repositoriesServiceSupplier.get().get()));
    }

    @Override
    public String getType() {
        return TYPE;
    }
}
