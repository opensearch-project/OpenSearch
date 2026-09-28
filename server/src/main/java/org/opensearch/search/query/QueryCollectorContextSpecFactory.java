/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Optional;

/**
 *  interface of QueryCollectorContext spec factory
 */
@ExperimentalApi
public interface QueryCollectorContextSpecFactory {
    /**
     * @param searchContext context needed to create collector context spec
     * @param query required to create collector context spec
     * @param queryCollectorArguments arguments to create collector context spec
     * @return QueryCollectorContextSpec
     * @throws IOException
     */
    Optional<QueryCollectorContextSpec> createQueryCollectorContextSpec(
        SearchContext searchContext,
        Query query,
        QueryCollectorArguments queryCollectorArguments
    ) throws IOException;
}
