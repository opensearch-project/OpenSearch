/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.knn.engine;

/**
 * Small data class for storing info that gets resolved during resolution process
 */
public interface ResolvedMethodContext {
    KNNMethodContext getKnnMethodContext();
    CompressionLevel getCompressionLevel();
}
