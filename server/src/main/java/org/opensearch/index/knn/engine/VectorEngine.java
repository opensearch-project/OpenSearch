/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

import java.util.Collections;
import java.util.List;

import org.apache.lucene.util.Version;

/**
 * VectorEngine is an interface that vector search engines in plugins must implement. It contains methods 
 * for getting the name and configurations of the engine
 */
public interface VectorEngine {
    /**
     * Gets the name of the KNNEngine.
     *
     * @return the name of the KNNEngine
     */
    String getName();

    /**
     * Gets the version of the library that is being used. In general, this can be used for ensuring compatibility of
     * serialized artifacts. For instance, this can be used to check if a given file that was created on a different
     * cluster is compatible with this instance of the library.
     *
     * @return the string representing the library's version
     */
    default String getVersion() {
        return Version.LATEST.toString();
    }
}
