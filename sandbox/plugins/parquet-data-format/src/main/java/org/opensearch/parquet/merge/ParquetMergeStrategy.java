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

/**
 * Interface defining a Parquet merge strategy.
 */
public interface ParquetMergeStrategy {

    /**
     * Performs the actual Parquet merge.
     */
    MergeResult mergeParquetFiles(MergeInput mergeInput);

}
