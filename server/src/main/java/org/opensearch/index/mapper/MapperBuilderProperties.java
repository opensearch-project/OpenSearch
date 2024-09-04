/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.index.compositeindex.datacube.DimensionType;

import java.util.Optional;

/**
 * An interface that defines properties for MapperBuilder implementations.
 *
 * @opensearch.experimental
 */
public interface MapperBuilderProperties {

    /**
     * Indicates whether the implementation supports data cube dimensions.
     *
     * @return an Optional containing the supported DimensionType if data cube dimensions are supported,
     *         or an empty Optional if not supported
     */
    default Optional<DimensionType> getSupportedDataCubeDimensionType() {
        return Optional.empty();
    }

    /**
     * Indicates whether the implementation supports data cube metrics.
     *
     * @return true if data cube metrics are supported, false otherwise
     */
    default boolean isDataCubeMetricSupported() {
        return false;
    }

}
