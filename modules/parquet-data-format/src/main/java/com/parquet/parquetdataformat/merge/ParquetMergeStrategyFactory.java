/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.merge;

/**
 * Factory for creating appropriate merge strategies based on compaction type.
 */
public class ParquetMergeStrategyFactory {

    public static ParquetMergeStrategy getStrategy(CompactionStrategy compactionStrategy) {
        switch (compactionStrategy) {
            case RECORD_BATCH:
            default:
                return new RecordBatchMergeStrategy();
        }
    }
}
