/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.action;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.indices.IndicesService;

/**
 * Guice-managed eager singleton that captures {@link IndicesService} for the
 * Parquet analyze REST action.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ParquetRegistryInitializer {

    private static volatile IndicesService indicesService;

    @Inject
    public ParquetRegistryInitializer(IndicesService indicesService) {
        ParquetRegistryInitializer.indicesService = indicesService;
    }

    public static IndicesService getIndicesService() {
        return indicesService;
    }
}
