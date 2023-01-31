/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.authz;

import org.opensearch.cluster.metadata.IndexNameExpressionResolver;

import java.util.Objects;

public class IndexNameExpressionResolverHolder {
    private static IndexNameExpressionResolver INSTANCE = null;

    /**
     * Do not allow instances of this class to be created
     */
    private IndexNameExpressionResolverHolder() {}

    /**
     * Gets the IndexNameExpressionResolver for this holder
     */
    public static IndexNameExpressionResolver getInstance() {
        Objects.requireNonNull(INSTANCE);
        return INSTANCE;
    }

    /**
     * ets the IndexNameExpressionResolver for this holder
     */
    public static void setIndexNameExpressionResolver(final IndexNameExpressionResolver indexNameExpressionResolver) {
        if (INSTANCE != null) {
            return;
        }
        INSTANCE = indexNameExpressionResolver;
    }
}
