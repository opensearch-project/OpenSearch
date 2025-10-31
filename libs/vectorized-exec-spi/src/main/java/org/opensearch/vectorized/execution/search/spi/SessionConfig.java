/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.search.spi;

public interface SessionConfig {

    default Integer getBatchSize(String indexName) {
        return null;
    }

    default Boolean isCollectStatisticsEnabled(String indexName) {
        return null;
    }

    default Boolean isPageIndexEnabled(String indexName) {
        return null;
    }

    void mergeFrom(SessionConfig other);

    long getOrCreateNativePtr();


}
