/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.stats;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;

/**
 * Common interface for shard-level data format statistics.
 * Defines the universal metrics every data format engine (Lucene, Parquet, etc.) emits.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DataFormatShardStats extends Writeable, ToXContentFragment {

    // Indexing
    long getDocsIndexedTotal();

    long getDocsIndexedFailures();

    long getIndexTimeMillis();

    // Merge
    long getMergeTotal();

    long getMergeTimeMillis();

    long getMergeFailures();
}
