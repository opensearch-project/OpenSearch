/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.composite.CompositeDataFormat;
import org.opensearch.composite.CompositeIndexingExecutionEngine;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.merge.DataFormatAwareMergePolicy;
import org.opensearch.index.engine.dataformat.merge.MergeHandler;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Factory for creating a {@link MergeHandler} configured for composite data formats.
 * <p>
 * Builds the per-format merger map, resolves primary/secondary formats, and creates
 * a {@link CompositeMergeExecutor} that orchestrates primary-then-secondary merges.
 * The resulting {@link MergeHandler} owns queue management; the executor owns execution.
 * <p>
 * Per-format plugins (Parquet, Lucene) implement {@link Merger} only — they don't
 * know about multi-format orchestration.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class CompositeMergeHandler {

    private CompositeMergeHandler() {}

    /**
     * Creates a {@link MergeHandler} for composite multi-format merges.
     *
     * @param engine             the composite engine providing primary and secondary delegates
     * @param compositeDataFormat the composite data format with primary format reference
     * @param snapshotSupplier   supplier for acquiring catalog snapshots
     * @param indexSettings      the index settings containing merge policy configuration
     * @param shardId            the shard ID for logging context
     * @return a configured MergeHandler
     */
    public static MergeHandler create(
        CompositeIndexingExecutionEngine engine,
        CompositeDataFormat compositeDataFormat,
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier,
        IndexSettings indexSettings,
        ShardId shardId
    ) {
        DataFormat primaryFormat = compositeDataFormat.getPrimaryDataFormat();
        List<DataFormat> secondaryFormats = resolveSecondaryFormats(engine, primaryFormat);
        Map<DataFormat, Merger> mergers = buildMergerMap(engine);
        CompositeMergeExecutor executor = new CompositeMergeExecutor(mergers);

        MergeHandler.MergeExecutor mergeExecutor = oneMerge -> {
            MergePlan plan = MergePlan.from(oneMerge, primaryFormat, secondaryFormats, engine.getNextWriterGeneration());
            return executor.execute(plan);
        };

        return new MergeHandler(
            snapshotSupplier,
            new DataFormatAwareMergePolicy(indexSettings.getMergePolicy(true), shardId),
            mergeExecutor,
            shardId
        );
    }

    private static List<DataFormat> resolveSecondaryFormats(
        CompositeIndexingExecutionEngine engine, DataFormat primaryFormat
    ) {
        List<DataFormat> secondaries = new ArrayList<>();
        for (IndexingExecutionEngine<?, ?> e : engine.getSecondaryDelegates()) {
            if (e.getDataFormat().equals(primaryFormat) == false) {
                secondaries.add(e.getDataFormat());
            }
        }
        return List.copyOf(secondaries);
    }

    private static Map<DataFormat, Merger> buildMergerMap(CompositeIndexingExecutionEngine engine) {
        Map<DataFormat, Merger> map = new HashMap<>();

        Merger primaryMerger = engine.getPrimaryDelegate().getMerger();
        if (primaryMerger == null) {
            throw new IllegalStateException(
                "Primary format [" + engine.getPrimaryDelegate().getDataFormat().name() + "] does not provide a Merger"
            );
        }
        map.put(engine.getPrimaryDelegate().getDataFormat(), primaryMerger);

        for (IndexingExecutionEngine<?, ?> secondary : engine.getSecondaryDelegates()) {
            Merger merger = secondary.getMerger();
            if (merger == null) {
                throw new IllegalStateException(
                    "Secondary format [" + secondary.getDataFormat().name() + "] does not provide a Merger"
                );
            }
            map.put(secondary.getDataFormat(), merger);
        }
        return Map.copyOf(map);
    }
}
