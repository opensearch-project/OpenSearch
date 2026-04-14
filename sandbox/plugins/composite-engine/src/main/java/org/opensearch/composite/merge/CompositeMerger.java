/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.composite.CompositeDataFormat;
import org.opensearch.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link Merger} that orchestrates composite merges across primary and secondary
 * data formats by delegating to {@link CompositeMergeExecutor}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeMerger implements Merger {

    private final DataFormat primaryFormat;
    private final List<DataFormat> secondaryFormats;
    private final CompositeMergeExecutor executor;

    public CompositeMerger(CompositeIndexingExecutionEngine engine, CompositeDataFormat compositeDataFormat) {
        this.primaryFormat = compositeDataFormat.getPrimaryDataFormat();
        this.secondaryFormats = resolveSecondaryFormats(compositeDataFormat, primaryFormat);
        this.executor = new CompositeMergeExecutor(buildMergerMap(engine));
    }

    @Override
    public MergeResult merge(MergeInput mergeInput) throws IOException {
        MergePlan plan = new MergePlan(
            mergeInput.newWriterGeneration(),
            primaryFormat,
            secondaryFormats,
            Map.of(primaryFormat, mergeInput.writerFiles())
        );
        return executor.execute(plan);
    }

    private static List<DataFormat> resolveSecondaryFormats(CompositeDataFormat compositeDataFormat, DataFormat primaryFormat) {
        List<DataFormat> secondaries = new ArrayList<>();
        for (DataFormat format : compositeDataFormat.getDataFormats()) {
            if (format.equals(primaryFormat) == false) {
                secondaries.add(format);
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
