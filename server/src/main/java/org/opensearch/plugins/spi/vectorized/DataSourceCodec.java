/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins.spi.vectorized;

/**
 * Service Provider Interface for DataFusion data source codecs.
 * Implementations provide access to different data formats (CSV, Parquet, etc.)
 * through the DataFusion query engine.
 */
public interface DataSourceCodec {

    /**
     * Returns the data format name
     */
    DataFormat getDataFormat();
}