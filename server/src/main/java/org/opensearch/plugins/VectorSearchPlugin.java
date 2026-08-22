/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import java.util.Collections;
import java.util.Map;

import org.opensearch.index.knn.engine.VectorEngine;

/**
 * An extension point for {@link Plugin} implementations to add custom knn engine implementations
 *
 * @opensearch.api
 */
public interface VectorSearchPlugin {
    default Map<String, VectorEngine> getKnnEngines() {
        return Collections.emptyMap();
    }
}
