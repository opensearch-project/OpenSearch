/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

/**
 * Represents a DataFusion query — wraps substrait plan bytes and execution metadata.
 */
public class DatafusionQuery {

    private final String indexName;
    private final byte[] substraitBytes;
    private boolean fetchPhase;

    public DatafusionQuery(String indexName, byte[] substraitBytes) {
        this.indexName = indexName;
        this.substraitBytes = substraitBytes;
    }

    public String getIndexName() {
        return indexName;
    }

    public byte[] getSubstraitBytes() {
        return substraitBytes;
    }

    public boolean isFetchPhase() {
        return fetchPhase;
    }

    public void setFetchPhase(boolean fetchPhase) {
        this.fetchPhase = fetchPhase;
    }
}
