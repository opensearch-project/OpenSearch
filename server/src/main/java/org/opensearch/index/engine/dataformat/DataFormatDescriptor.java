/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.store.FormatChecksumStrategy;

/**
 * Describes the static capabilities of a data format, including its default checksum
 * strategy and format name. Provided by {@link DataFormatPlugin} implementations and
 * consumed by DataFormatAwareStoreDirectory and DataFormatAwareRemoteDirectory.
 *
 * <p>The checksum strategy here is the <em>default fallback</em> — a full-file scan.
 * At runtime, the {@link IndexingExecutionEngine} may override this with a more
 * efficient strategy (e.g., {@link org.opensearch.index.store.PrecomputedChecksumStrategy})
 * via the shared checksum strategies map created during shard initialization.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DataFormatDescriptor {

    private final String formatName;
    private final FormatChecksumStrategy checksumStrategy;

    /**
     * Creates a new DataFormatDescriptor.
     *
     * @param formatName        the format name (e.g., "parquet")
     * @param checksumStrategy  the default checksum strategy for this format
     */
    public DataFormatDescriptor(String formatName, FormatChecksumStrategy checksumStrategy) {
        this.formatName = formatName;
        this.checksumStrategy = checksumStrategy;
    }

    /**
     * Returns the format name.
     *
     * @return the format name
     */
    public String getFormatName() {
        return formatName;
    }

    /**
     * Returns the default checksum strategy for this format.
     *
     * @return the checksum strategy
     */
    public FormatChecksumStrategy getChecksumStrategy() {
        return checksumStrategy;
    }
}
