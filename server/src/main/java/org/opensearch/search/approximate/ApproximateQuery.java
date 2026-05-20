/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.search.Query;
import org.opensearch.search.internal.SearchContext;

/**
 * Abstract class that can be inherited by queries that can be approximated. Queries should implement {@link #canApproximate(SearchContext)} to specify conditions on when they can be approximated
*/
public abstract class ApproximateQuery extends Query {

    protected abstract boolean canApproximate(SearchContext context);

}
