/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.schema.SchemaPlus;

/**
 * Provides a Calcite {@link SchemaPlus} from the current cluster state.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface SchemaProvider {

    /**
     * Builds a Calcite {@link SchemaPlus} from the given cluster state.
     *
     * @param clusterState the current cluster state (opaque Object to avoid
     *                     server dependency in the library)
     * @return a SchemaPlus with tables derived from index mappings
     */
    SchemaPlus buildSchema(Object clusterState);
}
