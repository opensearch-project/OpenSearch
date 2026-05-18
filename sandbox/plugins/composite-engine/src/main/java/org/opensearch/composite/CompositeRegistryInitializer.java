/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.composite.stats.CompositeStatsRegistry;
import org.opensearch.indices.IndicesService;

/**
 * Guice-managed eager singleton that wires {@link IndicesService} into the
 * {@link CompositeStatsRegistry} after node injection completes.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeRegistryInitializer {

    @Inject
    public CompositeRegistryInitializer(IndicesService indicesService) {
        CompositeStatsRegistry.getInstance().setIndicesService(indicesService);
    }
}
