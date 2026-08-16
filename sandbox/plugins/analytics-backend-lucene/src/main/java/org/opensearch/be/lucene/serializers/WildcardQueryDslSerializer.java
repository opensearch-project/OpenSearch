/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.opensearch.be.lucene.ConversionUtils;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;

import java.util.Map;

/**
 * Serializer for WILDCARD_QUERY_DSL — passes DSL-origin Lucene wildcard patterns verbatim, no SQL conversion.
 */
public class WildcardQueryDslSerializer extends AbstractRelevanceSerializer {

    @Override
    protected String functionName() {
        return "wildcard_query_dsl";
    }

    @Override
    protected QueryBuilder createQueryBuilder(ConversionUtils.RelevanceOperands operands) {
        // A DSL wildcard value is already a Lucene pattern — converting it would be a lossy round trip.
        return new WildcardQueryBuilder(operands.fieldName(), operands.query());
    }

    @Override
    protected void applyParams(QueryBuilder qb, Map<String, String> params) {
        WildcardQueryBuilder wildcardQb = (WildcardQueryBuilder) qb;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            switch (entry.getKey()) {
                case "case_insensitive" -> wildcardQb.caseInsensitive(Boolean.parseBoolean(entry.getValue()));
                case "rewrite" -> wildcardQb.rewrite(entry.getValue());
                default -> {
                    /* ignore unrecognized params for forward compatibility */ }
            }
        }
    }
}
