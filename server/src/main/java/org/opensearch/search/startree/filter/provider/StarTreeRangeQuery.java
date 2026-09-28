/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter.provider;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;

/**
 * Wrapper class for {@link RangeQueryBuilder} to expose the required fields
 */
@PublicApi(since = "3.1.0")
public class StarTreeRangeQuery {

    private final RangeQueryBuilder rangeQueryBuilder;

    StarTreeRangeQuery(QueryBuilder rq) {
        this.rangeQueryBuilder = (RangeQueryBuilder) rq;
    }

    public String fieldName() {
        return rangeQueryBuilder.fieldName();
    }

    public Object from() {
        return rangeQueryBuilder.from();
    }

    public Object to() {
        return rangeQueryBuilder.to();
    }

    public boolean includeLower() {
        return rangeQueryBuilder.includeLower();
    }

    public boolean includeUpper() {
        return rangeQueryBuilder.includeUpper();
    }

    public String format() {
        return rangeQueryBuilder.format();
    }

    public String timeZone() {
        return rangeQueryBuilder.timeZone();
    }
}
