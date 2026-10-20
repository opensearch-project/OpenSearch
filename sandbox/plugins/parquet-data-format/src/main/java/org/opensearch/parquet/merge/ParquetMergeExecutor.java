/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.merge;

import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;

/**
 * Executes Parquet merge operations using a pluggable {@link ParquetMergeStrategy}.
 */
public class ParquetMergeExecutor implements Merger {

    private final ParquetMergeStrategy strategy;

    public ParquetMergeExecutor(ParquetMergeStrategy strategy) {
        this.strategy = strategy;
    }

    @Override
    public MergeResult merge(MergeInput mergeInput) {
        return strategy.mergeParquetFiles(mergeInput);
    }
}
