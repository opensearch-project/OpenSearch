/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.merge;

import org.opensearch.common.collect.Tuple;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.merge.MergeResult;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Executes Parquet merge operations using a chosen compaction strategy.
 */
public class ParquetMergeExecutor extends ParquetMerger {

    private final ParquetMergeStrategy strategy;

    public ParquetMergeExecutor(CompactionStrategy compactionStrategy) {
        this.strategy = ParquetMergeStrategyFactory.getStrategy(compactionStrategy);
    }

    @Override
    public MergeResult merge(Collection<FileMetadata> fileMetadataList) {
        MergeResult result = strategy.mergeParquetFiles(fileMetadataList);
        strategy.postMerge();
        return result;
    }
}
