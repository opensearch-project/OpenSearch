/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

import java.util.Map;

/**
 * Holds the context needed to search a knn library.
 */
public interface KNNLibrarySearchContext {

    /**
     * Returns supported parameters for the library.
     *
     * @param ctx QueryContext
     * @return parameters supported by the library
     */
    Map<String, Parameter<?>> supportedMethodParameters(QueryContext ctx);
}
