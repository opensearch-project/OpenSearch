/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * {@link QueryPlanExecutor} default implementation.
 * <p>
 * Acquires a {@link DataFormatAwareEngine.DataFormatAwareReader} on the latest catalog snapshot,
 * then routes plan fragments to the appropriate {@link SearchExecEngine} per data format.
 * The composite reader holds the snapshot reference alive for the duration of the search.
 */
public class DefaultPlanExecutor implements QueryPlanExecutor<RelNode, Iterable<Object[]>> {

    private static final Logger logger = LogManager.getLogger(DefaultPlanExecutor.class);
    private final Map<String, AnalyticsSearchBackendPlugin> backEnds;
    private final IndicesService indicesService;
    private final ClusterService clusterService;

    public DefaultPlanExecutor(List<AnalyticsSearchBackendPlugin> plugins, IndicesService indicesService, ClusterService clusterService) {
        this.backEnds = new LinkedHashMap<>();
        for (AnalyticsSearchBackendPlugin plugin : plugins) {
            this.backEnds.put(plugin.name(), plugin);
        }
        this.indicesService = indicesService;
        this.clusterService = clusterService;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterable<Object[]> execute(RelNode logicalFragment, Object context) {
        // TODO : wire this properly , this is just to give an idea of flow
        AnalyticsSearchBackendPlugin plugin = selectBackEnd();
        String tableName = extractTableName(logicalFragment);
        DataFormatAwareEngine dataFormatAwareEngine = resolveCompositeEngine(tableName);

        List<DataFormat> formats = plugin.getSupportedFormats();
        DataFormat format = formats.get(0);

        // Acquire composite reader — incRefs the latest catalog snapshot.
        // Closing the reader decRefs the snapshot, allowing file cleanup.
        try (DataFormatAwareEngine.DataFormatAwareReader dataFormatAwareReader = dataFormatAwareEngine.acquireReader()) {
            Object reader = dataFormatAwareReader.getReader(format);
            SearchExecEngine searchEngine = dataFormatAwareEngine.getSearchExecEngine(format);
            Object plan = searchEngine.convertFragment(logicalFragment);
            var engineContext = searchEngine.createContext(reader, plan, null, null, null);
            Object result = searchEngine.execute(engineContext);

            // TODO: consume result stream into rows
            logger.info("[DefaultPlanExecutor] Executed via [{}]", plugin.name());
            return new ArrayList<>();
        } catch (Exception e) {
            throw new RuntimeException("Execution failed for [" + plugin.name() + "]", e);
        }
    }

    // TODO: Placeholder logic
    static String extractTableName(RelNode node) {
        if (node instanceof TableScan) {
            List<String> qn = node.getTable().getQualifiedName();
            return qn.get(qn.size() - 1);
        }
        for (RelNode input : node.getInputs()) {
            String name = extractTableName(input);
            if (name != null) return name;
        }
        throw new IllegalArgumentException("No TableScan found in plan fragment");
    }

    // TODO: Placeholder logic
    private DataFormatAwareEngine resolveCompositeEngine(String indexName) {
        IndexMetadata meta = clusterService.state().metadata().index(indexName);
        if (meta == null) throw new IllegalArgumentException("Index [" + indexName + "] not found");
        IndexService indexService = indicesService.indexService(meta.getIndex());
        if (indexService == null) throw new IllegalStateException("Index [" + indexName + "] not on this node");
        Set<Integer> shardIds = indexService.shardIds();
        if (shardIds.isEmpty()) throw new IllegalStateException("No shards for [" + indexName + "]");
        IndexShard shard = indexService.getShardOrNull(shardIds.iterator().next());
        if (shard == null) throw new IllegalStateException("Shard not found");
        DataFormatAwareEngine ce = shard.getCompositeEngine();
        if (ce == null) throw new IllegalStateException("No CompositeEngine on shard");
        return ce;
    }

    // TODO: Placeholder logic
    private AnalyticsSearchBackendPlugin selectBackEnd() {
        if (backEnds.isEmpty()) throw new IllegalStateException("No back-end plugins registered");
        return backEnds.values().iterator().next();
    }
}
