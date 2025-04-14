/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.OpenSearchException;

/**
 * Exception thrown when there is an issue with the hybrid search rescore query.
 */
public class HybridSearchRescoreQueryException extends OpenSearchException {

    public HybridSearchRescoreQueryException(Throwable cause) {
        super("rescore failed for hybrid query", cause);
    }
}
