/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.merge;

import org.opensearch.index.engine.exec.MergeInput;
import org.opensearch.index.engine.exec.merge.MergeResult;
/**
 * Executes Parquet merge operations using a chosen compaction strategy.
 */
public class ParquetMergeExecutor extends ParquetMerger {

    private final ParquetMergeStrategy strategy;
    private final String indexName;

    public ParquetMergeExecutor(CompactionStrategy compactionStrategy, String indexName) {
        this.strategy = ParquetMergeStrategyFactory.getStrategy(compactionStrategy);
        this.indexName = indexName;
    }

    @Override
    public MergeResult merge(MergeInput mergeInput) {
        MergeResult result = strategy.mergeParquetFiles(mergeInput);
        return result;
    }
}
