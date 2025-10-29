/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package com.parquet.parquetdataformat.merge;


import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.merge.MergeResult;

import java.util.Collection;
import java.util.List;

/**
 * Interface defining a Parquet merge strategy.
 */
public interface ParquetMergeStrategy {

    /**
     * Performs the actual Parquet merge.
     */
    MergeResult mergeParquetFiles(Collection<FileMetadata> files);

    /**
     * Optional post-merge hook.
     */
    default void postMerge() {
        // No-op by default
    }
}
